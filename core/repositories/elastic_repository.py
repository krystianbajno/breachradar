import logging
from typing import List

from elasticsearch import Elasticsearch, NotFoundError

from core.entities.elastic_chunk import ElasticChunk
from core.entities.scrap import Scrap
from core.repositories.postgres_repository import PostgresRepository


class ElasticRepository:
    def __init__(self, config, repository: PostgresRepository):
        self.logger = logging.getLogger(__name__)

        self.es = Elasticsearch(
            hosts=[{
                'host': config['host'],
                'port': config['port'],
                'scheme': config.get('scheme', 'http')
            }],
            http_auth=(config['user'], config['password']),
            timeout=60,
            verify_certs=False
        )

        self.postgres_repository: PostgresRepository = repository

    def save_scrap_chunk(self, elastic_chunk: ElasticChunk) -> str:
        try:
            response = self.es.index(index="scrapes_chunks", document={
                "scrap_id": elastic_chunk.scrap_id,
                "chunk_number": elastic_chunk.chunk_number,
                "content": elastic_chunk.chunk_content,
                "title": elastic_chunk.title,
                "hash": elastic_chunk.hash
            })
            elastic_id = response['_id']
            self.logger.info(
                f"Elastic chunk {elastic_chunk.chunk_number} for scrap {elastic_chunk.scrap_id} indexed in Elasticsearch with ID {elastic_id}."
            )
            return elastic_id
        except NotFoundError:
            self.logger.error("Index not found. Please create the index before indexing documents.")
            raise
        except Exception as e:
            self.logger.error(
                f"Failed to index elastic chunk {elastic_chunk.chunk_number} for scrap {elastic_chunk.scrap_id}: {e}"
            )
            raise

    def save_scrap_chunks(self, scrap: Scrap) -> List[str]:
        content = scrap.content
        title = scrap.filename
        hash_value = scrap.hash

        chunk_size = 1_000_000
        elastic_ids = []
        current_chunk = []
        current_chunk_size = 0

        def save_current_chunk():
            if current_chunk:
                chunk_content = ''.join(current_chunk)
                chunk_number = len(elastic_ids) + 1

                elastic_chunk = ElasticChunk(
                    scrap_id=scrap.id,
                    chunk_number=chunk_number,
                    chunk_content=chunk_content,
                    title=title,
                    hash=hash_value
                )

                elastic_id = self.save_scrap_chunk(elastic_chunk)
                self.postgres_repository.save_elastic_chunk(scrap.id, chunk_number, elastic_id, title)
                elastic_ids.append(elastic_id)

        for line in content.splitlines(keepends=True):
            line_size = len(line.encode('utf-8'))

            if current_chunk_size + line_size > chunk_size:
                save_current_chunk()
                current_chunk.clear()
                current_chunk_size = 0

            current_chunk.append(line)
            current_chunk_size += line_size

        save_current_chunk()
        return elastic_ids
