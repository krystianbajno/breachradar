from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class Scrap:
    id: Optional[int] = None
    hash: Optional[str] = None
    source: Optional[str] = None
    filename: Optional[str] = None
    file_path: Optional[str] = None
    state: str = 'PROCESSING'
    timestamp: Optional[str] = None
    content: Optional[bytes] = None
    occurrence_time: Optional[str] = None
    attachments: List = field(default_factory=list)
