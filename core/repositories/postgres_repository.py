import logging
import asyncpg
from datetime import datetime
from core.entities.scrap import Scrap

class PostgresRepository:
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.pool = None

    async def connect(self):
        try:
            self.pool = await asyncpg.create_pool(**self.config)
        except Exception as e:
            self.logger.error(f"Error connecting to PostgreSQL: {e}")
            raise

    async def save_elastic_chunk(self, scrap_id, chunk_number, elastic_id, title):
        query = """
        INSERT INTO elastic_chunks (scrap_id, chunk_number, elastic_id, title)
        VALUES ($1, $2, $3, $4)
        RETURNING id
        """
        try:
            async with self.pool.acquire() as conn:
                chunk_id = await conn.fetchval(query, scrap_id, chunk_number, elastic_id, title)
            self.logger.info(f"Elastic chunk {chunk_number} for scrap {scrap_id} saved successfully.")
            return chunk_id
        except Exception as e:
            self.logger.error(f"Failed to save elastic chunk {chunk_number} for scrap {scrap_id}: {e}")
            return None

    async def save_scrap_reference(self, scrap, state='PROCESSING'):
        processing_start_time = datetime.now() if state == 'PROCESSING' else None
        query = """
        INSERT INTO scrapes (hash, source, filename, scrape_time, file_path, state, timestamp, processing_start_time, occurrence_time)
        VALUES ($1, $2, $3, NOW(), $4, $5, $6, $7, $8)
        RETURNING id
        """
        try:
            async with self.pool.acquire() as conn:
                scrap_id = await conn.fetchval(query, scrap.hash, scrap.source, scrap.filename,
                    scrap.file_path, state, scrap.timestamp, processing_start_time, scrap.occurrence_time)
            self.logger.info(f"Scrap {scrap.hash} saved successfully with state '{state}' and id '{scrap_id}'.")
            return scrap_id
        except Exception as e:
            self.logger.error(f"Failed to save scrap {scrap.hash}: {e}")
            return None

    async def update_scrap_state(self, scrap_id, state):
        query = "UPDATE scrapes SET state = $1 WHERE id = $2"
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(query, state, scrap_id)
            self.logger.info(f"Scrap {scrap_id} updated to state '{state}'.")
        except Exception as e:
            self.logger.error(f"Failed to update scrap {scrap_id}: {e}")

    async def update_scrap_class(self, scrap_id: int, scrap_class: str):
        query = """
            UPDATE scrapes
            SET class = $1
            WHERE id = $2
        """
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(query, scrap_class, scrap_id)
                self.logger.info(f"Scrap ID {scrap_id} updated with class '{scrap_class}'.")
        except Exception as e:
            self.logger.error(f"Failed to update scrap class for scrap ID {scrap_id}: {e}")
            
    async def get_scrap_by_id(self, scrap_id):
        query = """
        SELECT id, hash, source, filename, file_path, state, timestamp, occurrence_time
        FROM scrapes
        WHERE id = $1
        """
        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchrow(query, scrap_id)
                if result:
                    return Scrap(
                        id=result['id'],
                        hash=result['hash'],
                        source=result['source'],
                        filename=result['filename'],
                        file_path=result['file_path'],
                        state=result['state'],
                        timestamp=result['timestamp'],
                        occurrence_time=result['occurrence_time'],
                    )
            return None
        except Exception as e:
            self.logger.error(f"Failed to fetch scrap by id {scrap_id}: {e}")
            return None

    async def get_unprocessed_scraps(self):
        query = """
        SELECT id, hash, source, filename, file_path, state, timestamp, occurrence_time
        FROM scrapes
        WHERE state IN ('NEW', 'PROCESSING')
        """
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query)
                return [
                    Scrap(
                        id=row['id'],
                        hash=row['hash'],
                        source=row['source'],
                        filename=row['filename'],
                        file_path=row['file_path'],
                        state=row['state'],
                        timestamp=row['timestamp'],
                        occurrence_time=row['occurrence_time'],
                    ) for row in rows
                ]
        except Exception as e:
            self.logger.error(f"Failed to fetch unprocessed scraps: {e}")
            return []

    async def get_processing_filenames(self):
        query = """
        SELECT filename
        FROM scrapes
        WHERE state = 'PROCESSING'
        """
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query)
                return [row['filename'] for row in rows]
        except Exception as e:
            self.logger.error(f"Failed to fetch processing filenames: {e}")
            return []

    async def get_classifier_patterns(self):
        query = "SELECT pattern, class FROM classifier_patterns"
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query)
                return [(row['pattern'], row["class"]) for row in rows]
        except Exception as e:
            self.logger.error(f"Failed to fetch credential patterns: {e}")
            return []

    async def is_hash_processed(self, file_hash):
        query = """
        SELECT EXISTS (
            SELECT 1 FROM scrapes
            WHERE hash = $1 AND state = 'PROCESSED'
        )
        """
        try:
            async with self.pool.acquire() as conn:
                exists = await conn.fetchval(query, file_hash)
            self.logger.info(f"Hash '{file_hash}' processed: {exists}")
            return bool(exists)
        except Exception as e:
            self.logger.error(f"Failed to check if hash '{file_hash}' is processed: {e}")
            return False


    async def delete_processing_scraps(self):
        query = "DELETE FROM scrapes WHERE state = 'PROCESSING'"
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(query)
            self.logger.info("Deleted all scraps in 'PROCESSING' state.")
        except Exception as e:
            self.logger.error(f"Failed to delete scraps in 'PROCESSING' state: {e}")
