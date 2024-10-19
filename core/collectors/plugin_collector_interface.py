from abc import ABC, abstractmethod

class PluginCollectorInterface(ABC):
    @abstractmethod
    async def collect(self):
        pass
