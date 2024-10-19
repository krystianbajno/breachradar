import logging
import time

from core.app import App
from core.config.config import Config
from core.ecs.ecs_manager import ECSManager
from core.providers.app_entity_provider import AppEntityProvider
from core.providers.app_service_provider import AppServiceProvider
from core.providers.app_system_provider import AppSystemProvider

def main():
    logging.basicConfig(level=logging.INFO)
    config = Config()
    app = App()

    app.bind('config', lambda: config)
    app.configuration = config

    app.register(AppServiceProvider)
    app.register(AppEntityProvider)
    app.register(AppSystemProvider)

    app.boot()

    ecs_manager = ECSManager(app)
    ecs_manager.run()

    while True:
        time.sleep(config.get("system_tick", 1))

if __name__ == "__main__":
    main()
