import logging
import time
from concurrent.futures import ThreadPoolExecutor

from core.collectors.plugin_collector_interface import PluginCollectorInterface
from core.events.event_system import EventSystem
from core.events.event_types import EventType

class CollectorSystem:
    def __init__(self, event_system: EventSystem, collectors, polling_interval: int = 1):
        self.logger = logging.getLogger(__name__)
        self.event_system = event_system
        self.collectors = collectors
        self.polling_interval = polling_interval

    def run(self):
        while True:
            with ThreadPoolExecutor() as executor:
                for collector in self.collectors:
                    executor.submit(self._run_collector, collector)
            time.sleep(self.polling_interval)

    def _run_collector(self, collector: PluginCollectorInterface):
        try:
            scraps = collector.collect()
            if not scraps:
                return
            for scrap in scraps:
                self.event_system.trigger_event(EventType.SCRAP_COLLECTED, scrap)
        except Exception as e:
            self.logger.exception(f"Error running collector {collector}: {e}")
