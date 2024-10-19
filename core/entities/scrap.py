from dataclasses import dataclass, field, asdict
from typing import List, Optional
import json
from datetime import datetime

@dataclass
class Scrap:
    id: Optional[int] = None
    hash: Optional[str] = None
    source: Optional[str] = None
    filename: Optional[str] = None
    file_path: Optional[str] = None
    state: str = 'PROCESSING'
    timestamp: Optional[datetime] = None  # Use datetime for timestamp
    occurrence_time: Optional[datetime] = None  # Use datetime for occurrence time
    attachments: List = field(default_factory=list)

    def to_json(self):
        dict_data = asdict(self)
        if dict_data['timestamp']:
            dict_data['timestamp'] = dict_data['timestamp'].isoformat()
        if dict_data['occurrence_time']:
            dict_data['occurrence_time'] = dict_data['occurrence_time'].isoformat()
        return json.dumps(dict_data)

    @staticmethod
    def from_json(data):
        dict_data = json.loads(data)
        if dict_data.get('timestamp'):
            dict_data['timestamp'] = datetime.fromisoformat(dict_data['timestamp'])
        if dict_data.get('occurrence_time'):
            dict_data['occurrence_time'] = datetime.fromisoformat(dict_data['occurrence_time'])
        return Scrap(**dict_data)
