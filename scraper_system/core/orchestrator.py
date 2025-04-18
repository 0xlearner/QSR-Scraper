import logging
import asyncio
import importlib
import inspect
from typing import Dict, Any, List, Type, Optional
from scraper_system.interfaces.fetcher_interface import FetcherInterface
from scraper_system.interfaces.parser_interface import ParserInterface
from scraper_system.interfaces.transformer_interface import TransformerInterface
from scraper_system.interfaces.storage_interface import StorageInterface

logger = logging.getLogger(__name__)

# Simple plugin registry (can be made more sophisticated later)
# Maps config names to actual plugin classes
PLUGIN_MAP = {
    # Fetchers
    "AsyncHTTPXFetcher": "scraper_system.plugins.fetchers.http_fetcher.AsyncHTTPXFetcher",
    # Parsers
    "GrilldParser": "scraper_system.plugins.parsers.grilld_parser.GrilldParser",
    # Transformers <--- ADDED
    "GrilldAddressTransformer": "scraper_system.plugins.transformers.grilld_address_transformer.GrilldAddressTransformer",
    # Storage
    "JSONStorage": "scraper_system.plugins.storage.json_storage.JSONStorage",
}

def get_plugin_class(plugin_name: str) -> Optional[Type]:
    """Dynamically imports and returns a plugin class."""
    if not plugin_name: # Handle cases where a plugin (like transformer) might be optional
        return None
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

    async def _scrape_url(
        self,
        url: str,
        site_name: str, # Pass site_name for context
        site_config: Dict[str, Any],
        fetcher: FetcherInterface,
        parser: ParserInterface,
        transformer: Optional[TransformerInterface], # Transformer is optional
        storage_plugins: List[StorageInterface]
        ):
            """Scrapes a single URL, parses, transforms, and stores the data."""
            logger.info(f"Starting scrape process for entry URL: {url} (Site: {site_name})")
            fetcher_config = site_config.get("config", {}).get("fetcher_options", {})
            # Pass the *entire* site config to the parser, it might need fetcher_options etc.
            parser_config = site_config.get("config", {})
            transformer_config = site_config.get("config", {}).get("transformer_options", {}) # Added
            storage_configs = site_config.get("config", {}).get("storage_options", {})

            # --- Initial Fetch ---
            initial_content, initial_content_type = await fetcher.fetch(url, fetcher_config)
            if initial_content is None:
                logger.warning(f"Failed to fetch initial content for {url}, skipping.")
                return

            # --- Parse ---
            try:
                # Pass the *full* config dict to parse, GrilldParser uses it
                parsed_data = await parser.parse(initial_content, initial_content_type, parser_config)
                if not parsed_data:
                    logger.info(f"Parser found no data starting from {url}.")
                    return # Nothing to transform or store
                logger.info(f"Parser returned {len(parsed_data)} raw items starting from {url}")
            except Exception as e:
                 logger.error(f"Parser execution failed starting from {url}: {e}", exc_info=True)
                 return

            # --- Transform (if transformer is configured) ---
            if transformer:
                try:
                    transformed_data = await transformer.transform(parsed_data, transformer_config, site_name)
                    if not transformed_data:
                        logger.info(f"Transformer returned no data for items from {url}.")
                        return # Nothing to store after transformation
                    logger.info(f"Transformer processed data, resulting in {len(transformed_data)} items from {url}")
                    data_to_store = transformed_data
                except Exception as e:
                     logger.error(f"Transformer execution failed for data from {url}: {e}", exc_info=True)
                     return # Stop processing this batch on transformer error
            else:
                 # If no transformer, store the raw parsed data
                 logger.debug(f"No transformer configured for site '{site_name}', storing raw parsed data.")
                 data_to_store = parsed_data


            # --- Store ---
            if not data_to_store:
                 logger.warning(f"No data left to store for {url} after parsing/transformation.")
                 return

            store_tasks = []
            for storage in storage_plugins:
                 storage_name = storage.__class__.__name__
                 specific_storage_config = storage_configs.get(storage_name, {})
                 store_tasks.append(storage.save(data_to_store, specific_storage_config))

            await asyncio.gather(*store_tasks)
            logger.info(f"Finished storing data originating from entry URL: {url}")


    async def _scrape_website(self, site_name: str, site_config: Dict[str, Any]):
        logger.info(f"Starting scrape process for website: {site_name}")

        # --- Load Plugins ---
        fetcher_cls = get_plugin_class(site_config.get("fetcher"))
        parser_cls = get_plugin_class(site_config.get("parser"))
        transformer_cls = get_plugin_class(site_config.get("transformer")) # Load transformer class (optional)
        storage_names = site_config.get("storage", [])
        storage_classes = [(name, get_plugin_class(name)) for name in storage_names]

        # Basic validation
        if not fetcher_cls:
            logger.error(f"Fetcher plugin '{site_config.get('fetcher')}' not loaded for '{site_name}'. Skipping.")
            return
        if not parser_cls:
            logger.error(f"Parser plugin '{site_config.get('parser')}' not loaded for '{site_name}'. Skipping.")
            return
        # Transformer is optional, so only log warning if specified but not loaded
        if site_config.get("transformer") and not transformer_cls:
             logger.error(f"Transformer plugin '{site_config.get('transformer')}' specified but not loaded for '{site_name}'. Skipping.")
             return
        if not all(cls for _, cls in storage_classes):
                logger.warning(f"One or more storage plugins failed to load for website '{site_name}'. Continuing without them.")
                storage_classes = [(name, cls) for name, cls in storage_classes if cls] # Filter out failed ones

        # --- Instantiate Plugins ---
        fetcher_instance: FetcherInterface = fetcher_cls()
        storage_instances: List[StorageInterface] = [cls() for name, cls in storage_classes if cls]

        # Instantiate Parser, potentially injecting the fetcher
        parser_instance: ParserInterface
        try:
            parser_init_signature = inspect.signature(parser_cls.__init__)
            parser_params = parser_init_signature.parameters
            if 'fetcher' in parser_params:
                    logger.debug(f"Injecting fetcher into {parser_cls.__name__}")
                    parser_instance = parser_cls(fetcher=fetcher_instance)
            else:
                    parser_instance = parser_cls()
        except Exception as e:
             logger.error(f"Failed to instantiate Parser {parser_cls.__name__}: {e}", exc_info=True)
             return

        # Instantiate Transformer (if configured)
        transformer_instance: Optional[TransformerInterface] = None
        if transformer_cls:
             try:
                 # Check if transformer needs injection (less common, but possible)
                 # transformer_init_signature = inspect.signature(transformer_cls.__init__)
                 # transformer_params = transformer_init_signature.parameters
                 # Add injection logic here if needed later
                 transformer_instance = transformer_cls()
                 logger.info(f"Instantiated transformer: {transformer_cls.__name__}")
             except Exception as e:
                 logger.error(f"Failed to instantiate Transformer {transformer_cls.__name__}: {e}", exc_info=True)
                 # Decide if we should continue without transformation or stop
                 logger.warning(f"Proceeding without transformation for site '{site_name}'.")
                 # transformer_instance = None # Explicitly set to None


        if not storage_instances:
                logger.warning(f"No valid storage plugins loaded or instantiated for '{site_name}'. Data will not be saved.")

        start_urls = site_config.get("start_urls", [])
        if not start_urls:
            logger.warning(f"No 'start_urls' defined for website '{site_name}'.")
            return

        # --- Create and Run Tasks ---
        tasks = []
        for url in start_urls:
            # Pass all required instances to _scrape_url
            tasks.append(self._scrape_url(
                 url,
                 site_name, # Pass site name
                 site_config,
                 fetcher_instance,
                 parser_instance,
                 transformer_instance, # Pass transformer (can be None)
                 storage_instances
                 ))

        await asyncio.gather(*tasks)
        logger.info(f"Finished scrape process for website: {site_name}")


    async def run(self):
        """Runs the scraping process for all configured websites."""
        logger.info("Orchestrator starting...")
        semaphore = asyncio.Semaphore(self.max_concurrent_workers)

        website_tasks = []
        websites = self.config.get("websites", {})

        if not websites:
            logger.warning("No websites configured in the 'websites' section.")
            return

        async def throttled_scrape(site_name, site_config):
             # Wrap _scrape_website call in semaphore
             # Add try/except block here to catch errors within a specific site's processing
             # without stopping the entire orchestrator run.
             try:
                 async with semaphore:
                    logger.debug(f"Acquired semaphore for site: {site_name}")
                    await self._scrape_website(site_name, site_config)
                    logger.debug(f"Released semaphore for site: {site_name}")
             except Exception as e:
                 # Catch errors not handled deeper down (e.g., plugin loading issues)
                 logger.error(f"Unhandled error during processing of site '{site_name}': {e}", exc_info=True)


        for site_name, site_config in websites.items():
            if not site_config.get("enabled", True):
                 logger.info(f"Website '{site_name}' is disabled. Skipping.")
                 continue
            # Pass site_name and config to the throttled function
            website_tasks.append(throttled_scrape(site_name, site_config))

        await asyncio.gather(*website_tasks)

        logger.info("Orchestrator finished.")
