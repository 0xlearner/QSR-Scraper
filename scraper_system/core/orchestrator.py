import logging
import asyncio
import importlib
import inspect
from typing import Dict, Any, List, Type, Optional
from scraper_system.interfaces.fetcher_interface import FetcherInterface
from scraper_system.interfaces.parser_interface import ParserInterface
from scraper_system.interfaces.storage_interface import StorageInterface

logger = logging.getLogger(__name__)

# Simple plugin registry (can be made more sophisticated later)
# Maps config names to actual plugin classes
PLUGIN_MAP = {
    # Fetchers
    "AsyncHTTPXFetcher": "scraper_system.plugins.fetchers.http_fetcher.AsyncHTTPXFetcher",
    # Parsers
    "GrilldParser": "scraper_system.plugins.parsers.grilld_parser.GrilldParser",
    # Storage
    "JSONStorage": "scraper_system.plugins.storage.json_storage.JSONStorage",
}

def get_plugin_class(plugin_name: str) -> Optional[Type]:
    """Dynamically imports and returns a plugin class."""
    if plugin_name not in PLUGIN_MAP:
        logger.error(f"Plugin '{plugin_name}' not found in PLUGIN_MAP.")
        return None

    module_path, class_name = PLUGIN_MAP[plugin_name].rsplit('.', 1)
    try:
        module = importlib.import_module(module_path)
        plugin_class = getattr(module, class_name)
        return plugin_class
    except ImportError:
        logger.error(f"Failed to import module {module_path} for plugin {plugin_name}.")
    except AttributeError:
        logger.error(f"Failed to find class {class_name} in module {module_path} for plugin {plugin_name}.")
    except Exception as e:
         logger.error(f"Error loading plugin {plugin_name}: {e}")
    return None


class Orchestrator:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.global_settings = config.get("global_settings", {})
        self.max_concurrent_workers = self.global_settings.get("max_concurrent_workers", 5) # Default concurrency

    async def _scrape_url(self, url: str, site_config: Dict[str, Any], fetcher: FetcherInterface, parser: ParserInterface, storage_plugins: List[StorageInterface]):
            """Scrapes a single URL or initiates a multi-step process via the parser."""
            logger.info(f"Starting scrape process for entry URL: {url}")
            fetcher_config = site_config.get("config", {}).get("fetcher_options", {})
            parser_config = site_config.get("config", {}).get("parser_options", {}) # Still pass for generic parsers
            storage_configs = site_config.get("config", {}).get("storage_options", {})

            # --- Initial Fetch ---
            # Use a separate config key if detail page fetches need different options
            initial_content, initial_content_type = await fetcher.fetch(url, fetcher_config)
            if initial_content is None:
                logger.warning(f"Failed to fetch initial content for {url}, skipping.")
                return

            # --- Parse (which might trigger more fetches internally) ---
            try:
                # The parser is now responsible for the entire data extraction logic for this entry URL,
                # potentially including fetching and parsing subsequent pages.
                # It should return the *final* list of extracted items.
                parsed_data = await parser.parse(initial_content, initial_content_type, parser_config) # Make parse potentially async
                if not parsed_data:
                    logger.info(f"No final data extracted starting from {url}.")
                    return # Nothing to store
            except Exception as e:
                 logger.error(f"Parser execution failed starting from {url}: {e}", exc_info=True)
                 return # Stop processing this entry point on parser error

            logger.info(f"Parser returned {len(parsed_data)} final items starting from {url}")

            # --- Store ---
            # This part remains the same, storing the final results returned by the parser
            store_tasks = []
            for storage in storage_plugins:
                 storage_name = storage.__class__.__name__
                 specific_storage_config = storage_configs.get(storage_name, {})
                 # Ensure save is awaited if it becomes async (which it already is)
                 store_tasks.append(storage.save(parsed_data, specific_storage_config))

            await asyncio.gather(*store_tasks)

            logger.info(f"Finished processing entry URL: {url}")


    async def _scrape_website(self, site_name: str, site_config: Dict[str, Any]):
        logger.info(f"Starting scrape process for website: {site_name}")

        # --- Load Plugins ---
        fetcher_cls = get_plugin_class(site_config.get("fetcher"))
        parser_cls = get_plugin_class(site_config.get("parser"))
        storage_names = site_config.get("storage", [])
        storage_classes = [(name, get_plugin_class(name)) for name in storage_names]

        if not fetcher_cls or not parser_cls or not all(cls for _, cls in storage_classes):
                logger.error(f"Failed to load required plugins for website '{site_name}'. Skipping.")
                return

        # --- Instantiate Plugins ---
        fetcher_instance: FetcherInterface = fetcher_cls()
        storage_instances: List[StorageInterface] = [cls() for name, cls in storage_classes if cls]

        # Instantiate Parser, potentially injecting the fetcher
        parser_instance: ParserInterface
        parser_init_signature = inspect.signature(parser_cls.__init__)
        parser_params = parser_init_signature.parameters

        if 'fetcher' in parser_params: # Check if parser's __init__ accepts 'fetcher'
                logger.debug(f"Injecting fetcher into {parser_cls.__name__}")
                parser_instance = parser_cls(fetcher=fetcher_instance)
        else:
                parser_instance = parser_cls()


        if not storage_instances:
                logger.error(f"No valid storage plugins loaded for '{site_name}'. Skipping storage.")
                # return # Optional: skip if no storage

        start_urls = site_config.get("start_urls", [])
        if not start_urls:
            logger.warning(f"No 'start_urls' defined for website '{site_name}'.")
            return

        # --- Create and Run Tasks ---
        # The _scrape_url task now handles the entire flow starting from a URL
        tasks = []
        for url in start_urls:
            # Pass instances needed by _scrape_url
            tasks.append(self._scrape_url(url, site_config, fetcher_instance, parser_instance, storage_instances))

        await asyncio.gather(*tasks)
        logger.info(f"Finished scrape process for website: {site_name}")


    async def run(self):
        """Runs the scraping process for all configured websites."""
        logger.info("Orchestrator starting...")
        semaphore = asyncio.Semaphore(self.max_concurrent_workers) # Global concurrency limit

        website_tasks = []
        websites = self.config.get("websites", {})

        if not websites:
            logger.warning("No websites configured in the 'websites' section.")
            return

        async def throttled_scrape(site_name, site_config):
             async with semaphore:
                 await self._scrape_website(site_name, site_config)

        for site_name, site_config in websites.items():
            if not site_config.get("enabled", True): # Allow disabling sites via config
                 logger.info(f"Website '{site_name}' is disabled. Skipping.")
                 continue
            website_tasks.append(throttled_scrape(site_name, site_config))

        await asyncio.gather(*website_tasks) # Run all website scraping tasks concurrently (respecting semaphore)

        logger.info("Orchestrator finished.")
