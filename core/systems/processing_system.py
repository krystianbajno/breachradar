import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.events.event_system import EventSystem
from core.events.event_types import EventType
from core.repositories.postgres_repository import PostgresRepository

class ProcessingSystem:
    def __init__(
        self,
        event_system: EventSystem,
        processors,
        repository: PostgresRepository,
        polling_interval: int = 1
    ):
        self.logger = logging.getLogger(__name__)
        self.event_system = event_system
        self.processors = processors
        self.repository = repository
        self.executor = ThreadPoolExecutor()
        self.polling_interval = polling_interval

        self.processing = {}
        self.processing_lock = threading.Lock()

        self.event_system.register_listener(EventType.SCRAP_COLLECTED, self.process_scrap)

    def run(self):
        while True:
            try:
                self._process_unprocessed_scraps()
            except Exception as e:
                self.logger.exception(f"Error processing scraps: {e}")
            time.sleep(self.polling_interval)

    def process_scrap(self, scrap):
        with self.processing_lock:
            if self.processing.get(scrap.id):
                self.logger.info(f"Scrap {scrap.id} is already being processed. Skipping.")
                return
            self.processing[scrap.id] = True

        applicable_processors = [p for p in self.processors if p.can_process(scrap)]
        futures = [
            self.executor.submit(self._run_processor, processor, scrap)
            for processor in applicable_processors
        ]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                self.logger.exception(f"Error processing scrap: {e}")

        with self.processing_lock:
            self.processing.pop(scrap.id, None)

    def _run_processor(self, processor, scrap):
        try:
            processor.process(scrap)
        except Exception as e:
            self.logger.exception(f"Error running processor {processor}: {e}")

    def _process_unprocessed_scraps(self):
        scraps = self.repository.get_unprocessed_scraps()

        for scrap in scraps:
            self.process_scrap(scrap)
