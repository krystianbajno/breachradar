from typing import Any, Callable, Dict, List

class App:
    def __init__(self):
        self.configuration = None
        self.providers = []
        self.services: Dict[str, Any] = {}
        self.systems = []
        self.entities: Dict[str, Any] = {}

    def add_system(self, system_factory: Callable):
        self.systems.append(system_factory(self))

    def get_system(self, system_name: str):
        for system in self.systems:
            if system.__class__.__name__ == system_name:
                return system
        raise ValueError(f"System '{system_name}' not found.")

    def add_entity(self, entity_factory: Callable):
        entity = entity_factory(self)
        self.entities[entity.get_id()] = entity

    def get_entity_by_id(self, identifier: str):
        return self.entities.get(identifier)

    async def register(self, provider_class):
        provider = provider_class(self)
        await provider.register()
        self.providers.append(provider)

    def bind(self, interface: str, factory: Callable, lazy: bool = False):
        if lazy:
            self.services[interface] = factory
        else:
            self.services[interface] = factory()

    def make(self, name: str):
        if name not in self.services:
            raise ValueError(f"Service '{name}' not registered.")
        service = self.services[name]
        if callable(service):
            return service()
        return service

    async def boot(self):
        for provider in self.providers:
            await provider.boot()