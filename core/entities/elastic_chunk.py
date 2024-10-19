from dataclasses import dataclass

@dataclass
class ElasticChunk:
    scrap_id: int
    chunk_number: int
    chunk_content: str
    title: str
    hash: str
