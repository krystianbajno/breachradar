from threading import Thread

from core.app import App
from core.events.event_system import EventSystem
from core.systems.collector_system import CollectorSystem
from core.systems.processing_system import ProcessingSystem


class ECSManager:
    def __init__(self, app: App):
        self.app = app
        self.event_system = self.app.make('EventSystem')

    def run(self):
        collector_system = self.app.get_system('CollectorSystem')
        processing_system = self.app.get_system('ProcessingSystem')

        collector_thread = Thread(target=collector_system.run, name='CollectorThread')
        processing_thread = Thread(target=processing_system.run, name='ProcessingThread')

        collector_thread.start()
        processing_thread.start()

        collector_thread.join()
        processing_thread.join()
