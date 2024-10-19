import logging
import os
import shutil
import zstandard as zstd

class LocalService:
    def __init__(self, directory: str, processed_directory: str):
        self.logger = logging.getLogger(__name__)
        self.directory = directory
        self.processed_directory = processed_directory

        os.makedirs(self.processed_directory, exist_ok=True)

    def fetch_scrape_files(self):
        scrape_files = []
        try:
            for root, _, files in os.walk(self.directory):
                for file in files:
                    file_path = os.path.join(root, file)
                    scrape_files.append({
                        "file_path": file_path,
                        "filename": os.path.basename(file_path),
                    })
            return scrape_files
        except Exception as e:
            self.logger.exception(f"Error fetching scrape files: {e}")
            return []

    def read_file_content(self, file_path: str) -> bytes:
        try:
            if file_path.endswith('.zst'):
                return self.decompress_zstd(file_path)
            else:
                with open(file_path, 'rb') as f:
                    return f.read()
        except Exception as e:
            self.logger.exception(f"Error reading file {file_path}: {e}")
            raise

    def decompress_zstd(self, file_path: str) -> bytes:
        try:
            with open(file_path, 'rb') as compressed_file:
                dctx = zstd.ZstdDecompressor()
                return dctx.decompress(compressed_file.read())
        except Exception as e:
            self.logger.exception(f"Error decompressing file {file_path}: {e}")
            raise

    def move_file_to_processed(self, file_path: str):
        try:
            if os.path.exists(file_path):
                destination_path = os.path.join(self.processed_directory, os.path.basename(file_path))
                shutil.move(file_path, destination_path)
                self.logger.info(f"Moved file {file_path} to {destination_path}")
            else:
                self.logger.warning(f"File {file_path} does not exist.")
        except Exception as e:
            self.logger.exception(f"Error moving file {file_path} to processed directory: {e}")
            raise
