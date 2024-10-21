import logging
import os

class MigrationService:
    def __init__(self, postgres_repository, migrations_dir='core/migrations'):
        self.logger = logging.getLogger(__name__)
        self.postgres_repository = postgres_repository
        self.migrations_dir = migrations_dir

    async def run_migrations_if_needed(self):
        async with self.postgres_repository.pool.acquire() as connection:
            try:
                await self._ensure_migrations_table(connection)
                applied_migrations = await self._get_applied_migrations(connection)
                migration_files = self._get_migration_files()

                for migration_file in migration_files:
                    if migration_file not in applied_migrations:
                        await self._apply_migration(connection, migration_file)
            except Exception as e:
                self.logger.error(f"Error running migrations: {e}")

    async def _ensure_migrations_table(self, connection):
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS migrations (
            id SERIAL PRIMARY KEY,
            migration_filename VARCHAR UNIQUE,
            applied_at TIMESTAMP DEFAULT NOW()
        );
        """
        try:
            await connection.execute(create_table_sql)
            self.logger.info("Ensured migrations table exists.")
        except Exception as e:
            self.logger.exception(f"Error ensuring migrations table exists: {e}")

    async def _get_applied_migrations(self, connection):
        query = "SELECT migration_filename FROM migrations;"
        try:
            records = await connection.fetch(query)
            applied_migrations = [record['migration_filename'] for record in records]
            return applied_migrations
        except Exception as e:
            self.logger.exception(f"Error fetching applied migrations: {e}")
            return []

    def _get_migration_files(self):
        migration_files = sorted(
            f for f in os.listdir(self.migrations_dir) if f.endswith('.sql')
        )

        return migration_files

    async def _apply_migration(self, connection, migration_file):
        migration_path = os.path.join(self.migrations_dir, migration_file)
        self.logger.info(f"Applying migration: {migration_file}")

        with open(migration_path, 'r') as file:
            migration_sql = file.read()

        try:
            async with connection.transaction():
                await connection.execute(migration_sql)
                await connection.execute(
                    "INSERT INTO migrations (migration_filename) VALUES ($1);",
                    migration_file
                )
            self.logger.info(f"Migration {migration_file} applied successfully.")
        except Exception as e:
            self.logger.exception(f"Error applying migration {migration_file}: {e}")
