import logging
from datetime import datetime

import psycopg2

from core.entities.scrap import Scrap


class PostgresRepository:
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.conn = self._connect()

    def _connect(self):
        try:
            return psycopg2.connect(**self.config)
        except psycopg2.Error as e:
            self.logger.error(f"Error connecting to PostgreSQL: {e}")
            raise

    def get_connection(self):
        if self.conn is None or self.conn.closed != 0:
            self.logger.info("Reconnecting to PostgreSQL...")
            self.conn = self._connect()
        return self.conn

    def save_elastic_chunk(self, scrap_id, chunk_number, elastic_id, title):
        query = """
        INSERT INTO elastic_chunks (scrap_id, chunk_number, elastic_id, title)
        VALUES (%s, %s, %s, %s)
        RETURNING id
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(query, (scrap_id, chunk_number, elastic_id, title))
                chunk_id = cursor.fetchone()[0]
            conn.commit()
            self.logger.info(
                f"Elastic chunk {chunk_number} for scrap {scrap_id} saved successfully."
            )
            return chunk_id
        except Exception as e:
            conn.rollback()
            self.logger.error(
                f"Failed to save elastic chunk {chunk_number} for scrap {scrap_id}: {e}"
            )
            return None

    def clear_scrap_content(self, scrap_id):
        query = "UPDATE scrapes SET content = NULL WHERE id = %s"
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(query, (scrap_id,))
            conn.commit()
            self.logger.info(f"Content for scrap {scrap_id} cleared.")
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to clear content for scrap {scrap_id}: {e}")

    def save_scrap_reference(self, scrap, state='PROCESSING'):
        processing_start_time = datetime.now() if state == 'PROCESSING' else None
        query = """
        INSERT INTO scrapes (hash, source, filename, scrape_time, file_path, state, timestamp, content, processing_start_time, occurrence_time)
        VALUES (%s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s)
        RETURNING id
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(query, (
                    scrap.hash,
                    scrap.source,
                    scrap.filename,
                    scrap.file_path,
                    state,
                    scrap.timestamp,
                    psycopg2.Binary(scrap.content) if scrap.content else None,
                    processing_start_time,
                    scrap.occurrence_time
                ))
                scrap_id = cursor.fetchone()[0]
            conn.commit()
            self.logger.info(
                f"Scrap {scrap.hash} saved successfully with state '{state}' and id '{scrap_id}'."
            )
            return scrap_id
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to save scrap {scrap.hash}: {e}")
            return None

    def update_scrap_state(self, scrap_id, state):
        query = "UPDATE scrapes SET state = %s WHERE id = %s"
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(query, (state, scrap_id))
            conn.commit()
            self.logger.info(f"Scrap {scrap_id} updated to state '{state}'.")
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to update scrap {scrap_id}: {e}")

    def get_scrap_by_id(self, scrap_id):
        query = """
        SELECT id, hash, source, filename, file_path, state, timestamp, content, occurrence_time
        FROM scrapes
        WHERE id = %s
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(query, (scrap_id,))
                result = cursor.fetchone()
                if result:
                    return Scrap(
                        id=result[0],
                        hash=result[1],
                        source=result[2],
                        filename=result[3],
                        file_path=result[4],
                        state=result[5],
                        timestamp=result[6],
                        content=result[7],
                        occurrence_time=result[8],
                    )
            return None
        except Exception as e:
            self.logger.error(f"Failed to fetch scrap by id {scrap_id}: {e}")
            return None

    def get_unprocessed_scraps(self):
        query = """
        SELECT id, hash, source, filename, file_path, state, timestamp, content, occurrence_time
        FROM scrapes
        WHERE (state = 'NEW') OR
              (state = 'PROCESSING')
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(query)
                scraps = cursor.fetchall()
                return [
                    Scrap(
                        id=row[0],
                        hash=row[1],
                        source=row[2],
                        filename=row[3],
                        file_path=row[4],
                        state=row[5],
                        timestamp=row[6],
                        content=row[7],
                        occurrence_time=row[8]
                    )
                    for row in scraps
                ]
        except Exception as e:
            self.logger.error(f"Failed to fetch unprocessed scraps: {e}")
            return []

    def get_processing_filenames(self):
        query = """
        SELECT filename
        FROM scrapes
        WHERE state = 'PROCESSING'
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(query)
                filenames = [row[0] for row in cursor.fetchall()]
            return filenames
        except Exception as e:
            self.logger.error(f"Failed to fetch processing filenames: {e}")
            return []

    def get_credential_patterns(self):
        query = "SELECT pattern FROM credential_patterns"
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(query)
                patterns = [row[0] for row in cursor.fetchall()]
            return patterns
        except Exception as e:
            self.logger.error(f"Failed to fetch credential patterns: {e}")
            return []

    def is_hash_processed(self, file_hash):
        query = """
        SELECT EXISTS (
            SELECT 1 FROM scrapes
            WHERE hash = %s AND state = 'PROCESSED'
        )
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(query, (file_hash,))
                exists = cursor.fetchone()[0]
            self.logger.info(f"Hash '{file_hash}' exists: {exists}")
            return exists
        except Exception as e:
            self.logger.error(f"Failed to check if hash '{file_hash}' is processed: {e}")
            return False

    def delete_processing_scraps(self):
        query = "DELETE FROM scrapes WHERE state = 'PROCESSING'"
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(query)
            conn.commit()
            self.logger.info("Deleted all scraps in 'PROCESSING' state.")
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to delete scraps in 'PROCESSING' state: {e}")
