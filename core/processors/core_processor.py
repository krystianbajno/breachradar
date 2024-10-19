import logging
from core.entities.scrap import Scrap
from core.repositories.elastic_repository import ElasticRepository
from core.repositories.postgres_repository import PostgresRepository
from rust_bindings import scan_file_for_patterns
import asyncio

class CoreProcessor:
    def __init__(self, postgres_repository: PostgresRepository, elastic_repository: ElasticRepository):
        self.logger = logging.getLogger(__name__)
        self.postgres_repository = postgres_repository
        self.elastic_repository = elastic_repository
        self.patterns = None

    async def process_scrap(self, scrap: Scrap):
        try:
            scrap_id = await self.postgres_repository.save_scrap_reference(scrap, 'PROCESSING')
            scrap.id = scrap_id
            
            self.logger.info(f"Processing scrap {scrap}.")

            if not scrap.hash:
                existing_scrap = await self.postgres_repository.get_scrap_by_id(scrap.id)
                if existing_scrap and existing_scrap.hash:
                    scrap.hash = existing_scrap.hash
                else:
                    self.logger.warning(f"No hash found for scrap {scrap}. Skipping processing.")
                    await self.postgres_repository.update_scrap_state(scrap.id, 'FAILED')

            if await self.postgres_repository.is_hash_processed(scrap.hash):
                self.logger.info(f"Hash {scrap.hash} has been processed before.")
                await self.postgres_repository.update_scrap_state(scrap.id, 'DUPLICATE_EXISTS')
            else:
                await self.postgres_repository.update_scrap_state(scrap.id, 'PROCESSED')
                await self.reinitialize()

                credentials_found = await self.check_for_credentials(scrap)
                if credentials_found:
                    await self.elastic_repository.save_scrap_chunks(scrap)

            self.logger.info(f"Scrap {scrap} processed successfully.")

        except Exception as e:
            await self.postgres_repository.update_scrap_state(scrap.id, 'FAILED')
            self.logger.exception(f"Error processing scrap {scrap}: {e}")

    async def reinitialize(self):
        self.patterns = await self._load_patterns()

    async def check_for_credentials(self, scrap):
        patterns = [p for p in self.patterns]
        matches = await asyncio.to_thread(scan_file_for_patterns, scrap.file_path, patterns)
        return matches if matches else None

    async def _load_patterns(self):
        patterns = await self.postgres_repository.get_credential_patterns()
        return patterns
