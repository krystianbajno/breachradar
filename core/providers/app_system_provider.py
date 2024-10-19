from core.events.event_system import EventSystem
from core.plugins.plugin_loader import PluginLoader
from core.systems.collector_system import CollectorSystem
from core.systems.processing_system import ProcessingSystem


class AppSystemProvider:
    def __init__(self, app):
        self.app = app
        self.plugin_loader = PluginLoader(app)

    def register(self):
        self.app.bind('EventSystem', lambda: EventSystem())

    def boot(self):
        self.plugin_loader.load_plugins()

        collectors = self.plugin_loader.get_plugins('collector')
        processors = self.plugin_loader.get_plugins('processor')

        collector_system = CollectorSystem(self.app.make('EventSystem'), collectors)

        processing_system = ProcessingSystem(
            self.app.make('EventSystem'),
            processors,
            self.app.make('PostgresRepository')
        )

        self.app.add_system(lambda app: collector_system)
        self.app.add_system(lambda app: processing_system)
