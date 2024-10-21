import logging
from elasticsearch import Elasticsearch, NotFoundError
from core.entities.elastic_chunk import ElasticChunk
from core.entities.scrap import Scrap
from core.repositories.postgres_repository import PostgresRepository
from rust_bindings import split_file_into_chunks
import asyncio

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

    async def save_scrap_chunks(self, scrap: Scrap):
        title = scrap.filename
        hash_value = scrap.hash
        chunk_size = 1_000_000

        try:
            chunks = split_file_into_chunks(scrap.file_path, chunk_size)

            tasks = []

            for chunk_number, chunk_content in chunks:
                elastic_chunk = ElasticChunk(
                    scrap_id=scrap.id,
                    chunk_number=chunk_number,
                    chunk_content=chunk_content,
                    title=title,
                    hash=hash_value
                )

                task = self.process_chunk(elastic_chunk, scrap.id, chunk_number, title)
                tasks.append(task)

            await asyncio.gather(*tasks)

        except Exception as e:
            self.logger.error(f"Failed to read file {scrap.file_path}: {e}")
            raise

    async def process_chunk(self, elastic_chunk: ElasticChunk, scrap_id: int, chunk_number: int, title: str):
        try:
            elastic_id = await self.save_scrap_chunk(elastic_chunk)

            await self.postgres_repository.save_elastic_chunk(scrap_id, chunk_number, elastic_id, title)

        except Exception as e:
            self.logger.error(f"Error processing chunk {chunk_number} for scrap {scrap_id}: {e}")
            raise

    async def save_scrap_chunk(self, elastic_chunk: ElasticChunk) -> str:
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
