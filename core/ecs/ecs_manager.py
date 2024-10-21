import asyncio
import logging
from core.app import App

class ECSManager:
    def __init__(self, app: App):
        self.app = app

    async def run(self):
        if self.app.configuration.get('collecting', True):
            collector_system = self.app.get_system('CollectorSystem')
            logging.info("Collector system enabled.")
            
        if self.app.configuration.get('processing', True):
            processing_system = self.app.get_system('ProcessingSystem')
            logging.info("Processing system enabled.")

        await asyncio.gather(
            collector_system.run(),
            processing_system.run()
        )