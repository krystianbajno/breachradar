from abc import ABC, abstractmethod


class PluginProvider(ABC):
    def __init__(self, app):
        self.app = app
        self.collectors = []
        self.processors = []

    @abstractmethod
    def register(self):
        pass
    
    @abstractmethod
    def boot(self):
        pass

    def get_collectors(self):
        return self.collectors

    def get_processors(self):
        return self.processors
