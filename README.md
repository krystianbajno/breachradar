# BreachRadar
```
    ____                       __    ____            __          
   / __ )________  ____ ______/ /_  / __ \____ _____/ /___ ______
  / __  / ___/ _ \/ __ `/ ___/ __ \/ /_/ / __ `/ __  / __ `/ ___/
 / /_/ / /  /  __/ /_/ / /__/ / / / _, _/ /_/ / /_/ / /_/ / /    
/_____/_/   \___/\__,_/\___/_/ /_/_/ |_|\__,_/\__,_/\__,_/_/     

                                              Krystian Bajno 2024
```

**BreachRadar** is an open-source Cyber Threat Intelligence (CTI) platform designed to collect and process data from various sources to detect potential security breaches and leaked credentials. It operates using ElasticSearch and a custom Python framework built on an Entity-Component-System (ECS) architecture with an event-driven design. The plugin system allows for easy integration of collection, processing, and analysis of new data sources.

# Features
- **Web Interface** - Provides a Web UI connected to ElasticSearch for searching data using keywords.
- **Plugin Support** - It is possible to extend functionality by adding new collectors, processors, and analyzers as plugins.
- **Event-Driven Processing** - Decouples collectors and processors using an event system.
- **Data persistence** - Uses PostgreSQL and Elasticsearch for data storage and searching.
- **Credential Detection** - Uses regex patterns stored in a database to detect leaked credentials.
- **Hashing and Tracking** - Hashes collected data and tracks origins based on the first occurrence of the same hash. Does not store same file twice, only a reference.

# Running
### Plugin Installation
Copy the plugin into `plugins/` directory. The framework will detect and run it automatically. To disable the plugin, navigate to plugin directory and edit `config.yaml`. Set `enabled` to `false`.

### Available plugins in core
**Due to sensitive nature of sources and operations, plugins are kept private and separate from core.**.
- **local_plugin** - Read data from the local storage - `./data/local_ingest` directory (default).

### Installation
0. Run `python3 -m venv venv`, `source venv/bin/activate`, and `pip install -r requirements.txt`
1. Run `docker-compose up` to start Kafka, PostgreSQL, and ElasticSearch.
2. Compile rust_bindings as they contain Rust PyO3, using `maturin build --release`.
3. Compile plugins if needed, as they may contain Rust PyO3, using `maturin build --release`.
3. Run `main.py` to setup the database, indexes, and start collection and processing service.
4. Run `npm install`, `npm run build`, and `npm run start` in `webui/` directory to start Web UI service.

You can distribute and scale these components on many machines in order to get a good performance through terabytes of data.

# Architecture Overview
The core system consists of the following main components:

- Collection and processing agent (`main.py`)
- ElasticSearch - Stores processed data and provides powerful search capabilities.
- Kafka - Is an event queue.
- PostgreSQL - Stores scrap metadata, tracks processing.
- WebUI - Allows to search and analyze data through a web interface connected to ElasticSearch.

### WebUI
- **WebUI** - Provides a `Next.js` based web UI connected to `ElasticSearch` for searching through data using keywords.

### Core
- **Core Processor** - Provides core functionalities like credential detection using regex patterns read from database and manages processing state.
- **Event System** - Manages communication between components through events.
- **Repositories** - Handles data storage in PostgreSQL and Elasticsearch.
- **Providers** - Manage the initialization and configuration of various system components and services.
- **Migrations** - Manage the database migrations.

### Plugins
- **Collectors** - Gather data from corresponding source.
- **Processors** - Process the collected data.
- **Analyzers** - Perform deeper analysis on processed data in order to highlight what is affected (for example domains, or usernames, or emails) and take action, for example send notification on detection of breach containing the pattern.

### Providers
Providers are responsible for registering and bootstrapping services, systems, and other components within the application. They make managing dependencies easy.

- **AppServiceProvider** - Registers core services like the event system, repositories, and core processors.
- **AppSystemProvider** - Initializes and bootstraps systems like collectors and processors, loading plugins as needed.
- **PluginProvider** - Base class for plugin providers; each plugin implements its own provider for registration.
- **MigrationServiceProvider** - Handles database migrations to ensure the schema is up-to-date.
- **AppEntityProvide** - Manages the registration of entities within the application.

# Technical details for development
### In order to develop a plugin
- Follow the directory structure of `local_plugin`.
- Collectors' classnames must end with `Collector`, Processors' classnames must end with `Processor`, Providers' classnames must end with `Provider`.
- Plugins must have a provider registering the plugin in `register` method and must extend `PluginProvider` class.
- In order to register and use a service inside plugin, use the `App` object passed to a plugin provider and the `.make()` and `.bind()` methods.

### Plugin `Collectors` and `Processors`
- Plugins can use `Core` components freely.
- Collectors implement **PluginCollectorInterface** and must define a `collect` method.
- Processors implement **PluginProcessorInterface** with `can_process` and `process` methods.
- Processors decide whether they can process a scrap based on `can_process`.

# Data Flow
### Event System
- **EventSystem** class manages events.
- Collectors trigger events like `SCRAP_COLLECTED`.
- Processors listen for events and process scraps accordingly.

### Collection
- Collectors gather data and create `Scrap` objects.
- Scraps are saved to PostgreSQL with state `PROCESSING`.
- An event `SCRAP_COLLECTED` is triggered.

### Processing
- Processors receive `SCRAP_COLLECTED` events.
- Process the scrap, then detect credentials using `CoreProcessor`.
- Update the scrap's state in PostgreSQL.
- Store processed data in Elasticsearch, and divide it into chunks, saving the reference in PostgreSQL.

### Credential Detection
- **CoreProcessor** loads regex patterns from the database and uses these patterns to search for credentials in content.

# TODO in core
- OpenCTI integration
- TheHive integration
- RecordedFuture integration
- Implement analysis and `basic_analysis` plugin.