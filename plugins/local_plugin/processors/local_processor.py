import logging

from core.entities.scrap import Scrap
from core.processors.plugin_processor_interface import PluginProcessorInterface
from plugins.local_plugin.services.local_service import LocalService
from core.processors.core_processor import CoreProcessor

class LocalProcessor(PluginProcessorInterface):
    def __init__(self, app):
        self.logger = logging.getLogger(__name__)
        self.local_service: LocalService = app.make('LocalService')
        self.core_processor: CoreProcessor = app.make('CoreProcessor')

    def can_process(self, scrap: Scrap) -> bool:
        return scrap.source == 'local'

    def process(self, scrap: Scrap):
        self.core_processor.process_scrap(scrap)

        try:
            self.local_service.move_file_to_processed(scrap.file_path)
            self.logger.info(f"Moved file {scrap.file_path} to processed directory.")
        except Exception as e:
            self.logger.exception(f"Error moving file {scrap.file_path}: {e}")
