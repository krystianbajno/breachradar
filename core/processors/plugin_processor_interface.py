from abc import ABC, abstractmethod
from core.entities.scrap import Scrap

class PluginProcessorInterface(ABC):
    @abstractmethod
    def can_process(self, scrap: Scrap) -> bool:
        pass

    @abstractmethod
    def process(self, scrap: Scrap):
        pass
