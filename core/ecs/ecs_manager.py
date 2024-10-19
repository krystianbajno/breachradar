import asyncio
from core.app import App

class ECSManager:
    def __init__(self, app: App):
        self.app = app

    async def run(self):
        collector_system = self.app.get_system('CollectorSystem')
        processing_system = self.app.get_system('ProcessingSystem')

        await asyncio.gather(
            collector_system.run(),
            processing_system.run()
        )