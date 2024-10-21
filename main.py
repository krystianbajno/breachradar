import logging
import asyncio
from core.app import App
from core.cli.logo import print_logo
from core.config.config import Config
from core.ecs.ecs_manager import ECSManager
from core.providers.app_entity_provider import AppEntityProvider
from core.providers.app_service_provider import AppServiceProvider
from core.providers.app_system_provider import AppSystemProvider

async def main():
    logging.basicConfig(level=logging.INFO)
    config = Config()
    app = App()
    

    app.bind('config', lambda: config)
    app.configuration = config
    
    await app.register(AppServiceProvider)
    await app.register(AppEntityProvider)
    await app.register(AppSystemProvider)

    await app.boot()
    
    ecs_manager = ECSManager(app)
    
    print_logo()

    await ecs_manager.run()

if __name__ == "__main__":
    asyncio.run(main())
