from abc import ABC, abstractmethod

class PluginCollectorInterface(ABC):
    @abstractmethod
    def collect(self):
        pass