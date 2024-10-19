import hashlib
import logging
import os
from datetime import datetime

from core.collectors.plugin_collector_interface import PluginCollectorInterface
from core.entities.scrap import Scrap
from core.repositories.postgres_repository import PostgresRepository
from plugins.local_plugin.services.local_service import LocalService

class LocalCollector(PluginCollectorInterface):
    def __init__(self, app):
        self.logger = logging.getLogger(__name__)
        self.local_service: LocalService = app.make('LocalService')
        self.repository: PostgresRepository = app.make('PostgresRepository')

    def collect(self):
        scrape_files = self.local_service.fetch_scrape_files()
        new_scraps = []
        if not scrape_files:
            self.logger.info("No new files to process.")
            return new_scraps

        processing_filenames = set(self.repository.get_processing_filenames())

        for file_info in scrape_files:
            filename = file_info['filename']
            file_path = file_info['file_path']

            if filename in processing_filenames:
                self.logger.info(f"File {filename} is already being processed. Skipping.")
                continue

            try:
                file_content = self.local_service.read_file_content(file_path)
                file_hash = self.calculate_hash(file_content)
            except Exception as e:
                self.logger.exception(f"Error reading or processing file {file_path}: {e}")
                continue

            occurrence_time = self._get_file_modification_time(file_path)
            creation_time = self._get_file_creation_time(file_path)

            scrap = self.create_scrap(
                file_hash,
                filename,
                file_path,
                file_content,
                creation_time,
                occurrence_time
            )

            scrap_id = self.repository.save_scrap_reference(scrap, state='PROCESSING')
            if scrap_id:
                scrap.id = scrap_id
                self.logger.info(f"File {filename} is marked as processing with id {scrap_id}.")
                processing_filenames.add(filename)
                new_scraps.append(scrap)
            else:
                self.logger.error(f"Failed to save scrap for file {filename}.")
        return new_scraps

    def create_scrap(self, file_hash, filename, file_path, file_content, creation_time, occurrence_time):
        return Scrap(
            hash=file_hash,
            source='local',
            filename=filename,
            file_path=file_path,
            timestamp=creation_time,
            content=file_content,
            occurrence_time=occurrence_time
        )

    @staticmethod
    def calculate_hash(file_content):
        return hashlib.sha256(file_content).hexdigest()

    def _get_file_creation_time(self, file_path):
        try:
            return datetime.fromtimestamp(os.path.getctime(file_path))
        except Exception:
            return None

    def _get_file_modification_time(self, file_path):
        try:
            return datetime.fromtimestamp(os.path.getmtime(file_path))
        except Exception:
            return None
