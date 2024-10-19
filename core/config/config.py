import logging
import os

from dotenv import load_dotenv
import yaml


class Config:
    def __init__(self, env_file='.env', config_file='config.yaml'):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )
        self.logger.addHandler(handler)

        load_dotenv(env_file)
        self.config_data = self._load_config_file(config_file)
        self._override_with_env_variables()

    def _load_config_file(self, config_file):
        if not os.path.exists(config_file):
            self.logger.warning(
                f"Config file {config_file} not found, using environment variables only."
            )
            return {}

        try:
            with open(config_file, 'r') as file:
                self.logger.info(f"Loading config from {config_file}")
                return yaml.safe_load(file) or {}
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing YAML config: {e}")
            return {}

    def _override_with_env_variables(self):
        for key, value in os.environ.items():
            if '__' in key:
                keys = key.lower().split('__')
                config_section = self.config_data
                for subkey in keys[:-1]:
                    config_section = config_section.setdefault(subkey, {})
                config_section[keys[-1]] = value
            else:
                self.config_data[key.lower()] = value

    def get(self, key, default=None):
        keys = key.split('.')
        config_value = self.config_data
        try:
            for k in keys:
                config_value = config_value[k]
            return config_value
        except KeyError:
            self.logger.warning(
                f"Key {key} not found in config, using default {default}"
            )
            return default

    def set(self, key, value):
        keys = key.split('.')
        config_section = self.config_data
        for k in keys[:-1]:
            config_section = config_section.setdefault(k, {})
        config_section[keys[-1]] = value

    def get_postgres_config(self):
        return {
            "database": self.get('postgres.database', 'cti_breach_hunter'),
            "user": self.get('postgres.user', 'cti_user'),
            "password": self.get('postgres.password', 'cti_password'),
            "host": self.get('postgres.host', 'localhost'),
            "port": self.get('postgres.port', '5432'),
        }

    def get_elasticsearch_config(self):
        return {
            "host": self.get('elasticsearch.host', 'localhost'),
            "port": int(self.get('elasticsearch.port', 9200)),
            "scheme": self.get('elasticsearch.scheme', 'http'),
            "user": self.get('elasticsearch.user', 'elastic'),
            "password": self.get('elasticsearch.password', 'password'),
        }
