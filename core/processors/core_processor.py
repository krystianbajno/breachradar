import logging
from core.entities.scrap import Scrap
from core.repositories.elastic_repository import ElasticRepository
from core.repositories.postgres_repository import PostgresRepository
from rust_bindings import process_scrap_in_rust
import asyncio


class CoreProcessor:
    def __init__(self, postgres_repository: PostgresRepository, elastic_repository: ElasticRepository):
        self.logger = logging.getLogger(__name__)
        self.postgres_repository = postgres_repository
        self.elastic_repository = elastic_repository
        self.patterns = None

    async def process_scrap(self, scrap: Scrap):
        try:
            scrap.id = await self._initialize_scrap(scrap)

            scrap_hash_task = self._ensure_scrap_hash(scrap)
            load_patterns_task = self._load_patterns_if_needed()
            scrap.hash, _ = await asyncio.gather(scrap_hash_task, load_patterns_task)
            
            if not scrap.hash:
                return
            
            is_hash_processed = await self.hash_exists(scrap.hash)

            result = await asyncio.to_thread(
                process_scrap_in_rust,
                scrap.file_path,
                self.patterns,
                is_hash_processed
            )

            if not result:
                await self._handle_no_patterns(scrap, is_hash_processed)
                return

            scrap_class, matches = result
            await self._handle_patterns_found(scrap, scrap_class) #, matches)

            await self._finalize_scrap(scrap, 'PROCESSED')

        except Exception as e:
            await self._finalize_scrap(scrap, 'FAILED')
            self.logger.exception(f"Error processing scrap {scrap}: {e}")

    async def _initialize_scrap(self, scrap: Scrap) -> int:
        scrap_id = await self.postgres_repository.save_scrap_reference(scrap, 'PROCESSING')
        self.logger.info(f"Initialized scrap with ID {scrap_id}.")
        return scrap_id

    async def _ensure_scrap_hash(self, scrap: Scrap) -> str:
        if scrap.hash:
            return scrap.hash

        existing_scrap = await self.postgres_repository.get_scrap_by_id(scrap.id)
        if existing_scrap and existing_scrap.hash:
            self.logger.info(f"Recovered hash for scrap {scrap.id}.")
            return existing_scrap.hash

        self.logger.warning(f"Hash missing for scrap {scrap.id}, marking as failed.")
        await self._finalize_scrap(scrap, 'FAILED')
        return None

    async def _load_patterns_if_needed(self):
        if not self.patterns:
            self.patterns = await self._load_patterns()
        return self.patterns

    async def _load_patterns(self):
        patterns = await self.postgres_repository.get_classifier_patterns()
        self.logger.info(f"Loaded {len(patterns)} patterns.")

        return [(p[0], p[1]) for p in patterns]

    async def _handle_no_patterns(self, scrap: Scrap, is_hash_processed: bool):
        if is_hash_processed:
            await self._finalize_scrap(scrap, 'DUPLICATE_EXISTS')
        else:
            await self._finalize_scrap(scrap, 'NO_PATTERNS_FOUND')

    async def _handle_patterns_found(self, scrap: Scrap, scrap_class: str): #, matches: list):
        await asyncio.gather(
            self.postgres_repository.update_scrap_class(scrap.id, scrap_class),
            self.elastic_repository.save_scrap_chunks(scrap) #, matches)
        )
        self.logger.info(f"Patterns found for scrap {scrap.id}, class updated to '{scrap_class}'.")

    async def _finalize_scrap(self, scrap: Scrap, state: str):
        await self.postgres_repository.update_scrap_state(scrap.id, state)
        self.logger.info(f"Scrap {scrap.id} marked as {state}.")

    async def hash_exists(self, hash):
        return await self.postgres_repository.is_hash_processed(hash)