from core.providers.plugin_provider import PluginProvider
from plugins.local_plugin.collectors.local_collector import LocalCollector
from plugins.local_plugin.processors.local_processor import LocalProcessor
from plugins.local_plugin.services.local_service import LocalService

class LocalPluginProvider(PluginProvider):
    def register(self):
        self.app.bind('LocalService', lambda: LocalService(
            self.app.configuration.get('local_plugin.watch_directory', './data/local_ingest'),
            self.app.configuration.get('local_plugin.processed_directory', './data/local_ingest_processed'),
        ))

        local_collector = LocalCollector(self.app)
        local_processor = LocalProcessor(self.app)

        self.collectors.append(local_collector)
        self.processors.append(local_processor)

    def boot(self):
        pass