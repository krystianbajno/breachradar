from enum import Enum

class EventType(Enum):
    SCRAP_COLLECTED = 'SCRAP_COLLECTED'
    SCRAP_PROCESSED = 'SCRAP_PROCESSED'
    POLL = 'POLL'
