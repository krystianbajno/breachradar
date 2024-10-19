import logging
import re

from core.entities.scrap import Scrap
from core.repositories.elastic_repository import ElasticRepository
from core.repositories.postgres_repository import PostgresRepository


class CoreProcessor:
    def __init__(self, postgres_repository: PostgresRepository, elastic_repository: ElasticRepository):
        self.logger = logging.getLogger(__name__)
        self.postgres_repository = postgres_repository
        self.elastic_repository = elastic_repository
        self.patterns = None
        
    def reinitialize(self):
        self.patterns = self._load_patterns()

    def process_scrap(self, scrap: Scrap):
        try:
            self.logger.info(f"Processing scrap with id {scrap.id}.")

            if not scrap.hash:
                existing_scrap = self.postgres_repository.get_scrap_by_id(scrap.id)
                if existing_scrap and existing_scrap.hash:
                    scrap.hash = existing_scrap.hash
                else:
                    self.logger.warning(
                        f"No hash found for scrap {scrap.id}. Skipping processing."
                    )
                    self.postgres_repository.update_scrap_state(scrap.id, 'FAILED')
                    return

            if self.postgres_repository.is_hash_processed(scrap.hash):
                self.logger.info(f"Hash {scrap.hash} has been processed before.")
                self.postgres_repository.update_scrap_state(scrap.id, 'DUPLICATE_EXISTS')
            else:
                self._process_scrap_content(scrap)
                credentials_found = self.check_for_credentials(scrap.content)
                if credentials_found:
                    self.elastic_repository.save_scrap_chunks(scrap)
                self.postgres_repository.update_scrap_state(scrap.id, 'PROCESSED')

            self.postgres_repository.clear_scrap_content(scrap.id)
            self.logger.info(f"Scrap with id {scrap.id} processed successfully.")

        except Exception as e:
            self.postgres_repository.update_scrap_state(scrap.id, 'FAILED')
            self.logger.exception(f"Error processing scrap with id {scrap.id}: {e}")

    def check_for_credentials(self, content):
        self.reinitialize()

        credentials = []

        for pattern in self.patterns:
            matches = pattern.findall(content)
            credentials.extend(matches)

        return credentials if credentials else None

    def _load_patterns(self):
        patterns = self.postgres_repository.get_credential_patterns()
        compiled_patterns = [re.compile(pattern) for pattern in patterns]
        return compiled_patterns

    def _process_scrap_content(self, scrap: Scrap):
        try:
            file_content = scrap.content.decode('utf-8', errors='replace')
            scrap.content = file_content
        except Exception:
            self.logger.warning(
                f"Invalid content format for scrap {scrap.id}. Skipping processing."
            )