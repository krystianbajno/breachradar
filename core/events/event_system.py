import logging
from typing import Any, Callable, Dict, List

class EventSystem:
    def __init__(self):
        self.listeners: Dict[str, List[Callable]] = {}
        self.logger = logging.getLogger(__name__)

    def register_listener(self, event_name: str, callback: Callable):
        self.listeners.setdefault(event_name, []).append(callback)

    def trigger_event(self, event_name: str, *args, **kwargs):
        for callback in self.listeners.get(event_name, []):
            try:
                callback(*args, **kwargs)
            except Exception as e:
                self.logger.exception(f"Error in event listener for {event_name}: {e}")
