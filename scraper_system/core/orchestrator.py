import logging
import asyncio
from typing import Dict, Any, List, Type, Optional, Tuple

from scraper_system.interfaces.fetcher_interface import FetcherInterface
from scraper_system.interfaces.parser_interface import ParserInterface
from scraper_system.interfaces.transformer_interface import TransformerInterface
from scraper_system.interfaces.storage_interface import StorageInterface
from scraper_system.plugins.parsers.kfc_parser import KfcParser
from scraper_system.core.plugin_factory import PluginFactory

logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.global_settings = config.get("global_settings", {})
        self.max_concurrent_workers = self.global_settings.get(
            "max_concurrent_workers", 5
        )  # Default concurrency
        self.plugin_factory = PluginFactory(config)
        self.storage_plugins = []  # Track active storage plugins

    def _load_and_validate_plugins(self, site_name: str, site_config: Dict[str, Any]):
        """Load and validate plugins using the plugin factory"""
        try:
            return self.plugin_factory.create_plugins_for_site(site_name, site_config)
        except Exception as e:
            logger.error(f"Failed to load plugins for site '{site_name}': {e}")
            return None

    async def _scrape_url(
        self,
        url: str,
        site_name: str,
        site_config: Dict[str, Any],
        fetcher: FetcherInterface,
        parser: ParserInterface,
        transformer: Optional[TransformerInterface],
        storage_plugins: List[StorageInterface],
    ):
        """
        Scrapes a single URL, parses, transforms, and stores the data.
        """
        logger.info(f"Starting scrape process for entry URL: {url} (Site: {site_name})")
        parser_config = site_config.get("config", {})
        transformer_config = site_config.get("config", {}).get(
            "transformer_options", {}
        )
        storage_configs = site_config.get("config", {}).get("storage_options", {})

        # Add the initial fetch here
        fetcher_config = site_config.get("config", {}).get("fetcher_options", {})
        content, content_type, status_code = await fetcher.fetch(url, fetcher_config)

        if not content:
            logger.error(
                f"Failed to fetch initial content from {url} for site '{site_name}' (Status: {status_code})"
            )
            return

        # --- Parse ---
        try:
            parsed_data = await parser.parse(
                content=content, content_type=content_type, config=parser_config
            )
            if not parsed_data:
                logger.info(
                    f"Parser {parser.__class__.__name__} found no data for site '{site_name}'."
                )
                return
            logger.info(
                f"Parser {parser.__class__.__name__} returned {len(parsed_data)} raw items for site '{site_name}'"
            )
        except Exception as e:
            logger.error(
                f"Parser execution failed for site '{site_name}': {e}", exc_info=True
            )
            return

        # --- Transform (if transformer is configured) ---
        data_to_store = parsed_data  # Default to parsed data
        if transformer:
            try:
                transformed_data = await transformer.transform(
                    parsed_data, transformer_config, site_name
                )
                if not transformed_data:
                    logger.info(
                        f"Transformer returned no data for items from site '{site_name}'."
                    )
                    return
                logger.info(
                    f"Transformer processed data, resulting in {len(transformed_data)} items from site '{site_name}'"
                )
                data_to_store = transformed_data
            except Exception as e:
                logger.error(
                    f"Transformer execution failed for data from site '{site_name}': {e}",
                    exc_info=True,
                )
                return
        else:
            logger.debug(
                f"No transformer configured for site '{site_name}', using raw parsed data."
            )

        # --- Store ---
        if not data_to_store:
            logger.warning(
                f"No data left to store for site '{site_name}' after parsing/transformation."
            )
            return

        store_tasks = []
        for storage in storage_plugins:
            storage_name = storage.__class__.__name__
            specific_storage_config = storage_configs.get(storage_name, {})
            store_tasks.append(
                asyncio.ensure_future(
                    storage.save(data_to_store, specific_storage_config)
                )
            )

        if store_tasks:
            results = await asyncio.gather(*store_tasks, return_exceptions=True)
            for idx, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(
                        f"Storage plugin {storage_plugins[idx].__class__.__name__} failed: {result}",
                        exc_info=False,
                    )
            logger.info(f"Finished storing data originating from site: {site_name}")
        else:
            logger.warning(
                f"No storage plugins available to store data for site '{site_name}'"
            )

        # Close storage connections after use
        close_tasks = []
        for storage in storage_plugins:
            if hasattr(storage, "close") and callable(storage.close):
                close_tasks.append(asyncio.ensure_future(storage.close()))

        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)

    # --- Refactored Helper Methods ---

    def _instantiate_transformer(
        self,
        site_name: str,
        site_config: Dict[str, Any],
        transformer_cls: Type[TransformerInterface],
    ) -> Optional[TransformerInterface]:
        """Instantiates the transformer plugin, handling API key logic."""
        try:
            transformer_config = site_config.get("config", {}).get(
                "transformer_options", {}
            )
            api_key = transformer_config.get("api_key")

            # Check if the transformer expects an api_key in its __init__
            import inspect

            init_signature = inspect.signature(transformer_cls.__init__)
            takes_api_key = "api_key" in init_signature.parameters

            instance = None
            if takes_api_key:
                if api_key:
                    logger.info(
                        f"Instantiating transformer {transformer_cls.__name__} with API key for '{site_name}'."
                    )
                    instance = transformer_cls(api_key=api_key)
                else:
                    # Transformer expects api_key but none provided in config
                    logger.error(
                        f"Transformer {transformer_cls.__name__} requires an 'api_key' in transformer_options for site '{site_name}', but none was found. Cannot instantiate."
                    )
                    return None
            else:
                if api_key:
                    logger.warning(
                        f"API key provided for transformer {transformer_cls.__name__} in site '{site_name}', but the transformer does not accept an 'api_key' argument. Instantiating without it."
                    )
                else:
                    logger.info(
                        f"Instantiating transformer {transformer_cls.__name__} without API key for '{site_name}'."
                    )
                instance = transformer_cls()

            if instance:
                logger.info(
                    f"Successfully instantiated transformer: {transformer_cls.__name__} for '{site_name}'"
                )
            return instance

        except TypeError as e:
            logger.error(
                f"TypeError instantiating Transformer {transformer_cls.__name__} for site '{site_name}': {e}. Check config and class __init__.",
                exc_info=True,
            )
            return None
        except Exception as e:
            logger.error(
                f"Failed to instantiate Transformer {transformer_cls.__name__} for site '{site_name}': {e}",
                exc_info=True,
            )
            return None

    def _instantiate_storage_plugins(
        self, site_name: str, storage_classes: List[Tuple[str, Type[StorageInterface]]]
    ) -> List[StorageInterface]:
        """Instantiates storage plugins, logging errors for failed ones."""
        instances = []
        for name, cls in storage_classes:
            try:
                instances.append(cls())
            except Exception as e:
                logger.error(
                    f"Failed to instantiate Storage plugin {cls.__name__} (config name: {name}) for site '{site_name}': {e}",
                    exc_info=True,
                )
        return instances

    def _instantiate_plugins(
        self,
        site_name: str,
        site_config: Dict[str, Any],
        fetcher_cls: Type[FetcherInterface],
        parser_cls: Type[ParserInterface],
        transformer_cls: Optional[Type[TransformerInterface]],
        storage_classes: List[Tuple[str, Type[StorageInterface]]],
    ) -> Optional[
        Tuple[
            FetcherInterface,
            ParserInterface,
            Optional[TransformerInterface],
            List[StorageInterface],
        ]
    ]:
        """Instantiates the loaded plugin classes using helper methods."""
        fetcher = None
        parser = None
        transformer_instance = None
        storage_instances = []

        try:
            fetcher = fetcher_cls()
        except Exception as e:
            logger.error(
                f"Failed to instantiate Fetcher {fetcher_cls.__name__} for site '{site_name}': {e}",
                exc_info=True,
            )
            return None

        try:
            # Pass the fetcher instance to the parser constructor
            parser = parser_cls(fetcher=fetcher)
        except Exception as e:
            logger.error(
                f"Failed to instantiate Parser {parser_cls.__name__} for site '{site_name}': {e}",
                exc_info=True,
            )
            return None  # Parser instantiation is critical

        if transformer_cls:
            transformer_instance = self._instantiate_transformer(
                site_name, site_config, transformer_cls
            )
            if transformer_instance is None:
                logger.error(
                    f"Transformer instantiation failed for site '{site_name}', skipping site."
                )
                return None  # Transformer instantiation is critical if specified

        storage_instances = self._instantiate_storage_plugins(
            site_name, storage_classes
        )

        if not storage_classes:
            logger.info(f"No storage plugins configured for '{site_name}'.")
        elif not storage_instances:
            logger.warning(
                f"No storage plugins successfully instantiated for '{site_name}'. Data will not be saved."
            )

        return fetcher, parser, transformer_instance, storage_instances

    async def _scrape_website(
        self,
        site_name: str,
        site_config: Dict[str, Any],
        fetcher: FetcherInterface,
        parser: ParserInterface,
        transformer: Optional[TransformerInterface],
        storage_plugins: List[StorageInterface],
    ):
        """Scrapes a single website configuration, handling all URLs and storing results."""
        logger.info(f"Starting scrape process for website: {site_name}")

        # Special handling for KfcParser
        if isinstance(parser, KfcParser):
            try:
                parser_config = site_config.get("config", {})
                parsed_data = await parser.parse(
                    content=None, content_type=None, config=parser_config
                )

                if parsed_data:
                    # Handle transformation if configured
                    data_to_store = parsed_data
                    if transformer:
                        transformer_config = parser_config.get(
                            "transformer_options", {}
                        )
                        data_to_store = await transformer.transform(
                            parsed_data, transformer_config, site_name
                        )

                    # Store the data
                    if data_to_store:
                        storage_configs = parser_config.get("storage_options", {})
                        for storage in storage_plugins:
                            storage_config = storage_configs.get(
                                storage.__class__.__name__, {}
                            )
                            await storage.save(data_to_store, storage_config)

                return
            except Exception as e:
                logger.error(
                    f"Error processing KFC parser for site '{site_name}': {e}",
                    exc_info=True,
                )
                return

        # Regular handling for other parsers
        start_urls = site_config.get("start_urls", [])
        if not start_urls:
            logger.warning(f"No start URLs configured for site: {site_name}")
            return

        for url in start_urls:
            await self._scrape_url(
                url=url,
                site_name=site_name,
                site_config=site_config,
                fetcher=fetcher,
                parser=parser,
                transformer=transformer,
                storage_plugins=storage_plugins,
            )

        logger.info(f"Finished scrape process for website: {site_name}")

    async def run(self):
        """Main entry point to run the scraper orchestrator."""
        logger.info("Orchestrator starting...")
        websites = self.config.get("websites", {})
        website_tasks = []
        semaphore = asyncio.Semaphore(self.max_concurrent_workers)

        async def throttled_scrape(site_name, site_config):
            async with semaphore:
                logger.debug(f"Acquired semaphore for site: {site_name}")
                try:
                    # Load and validate plugins first
                    plugin_instances = self._load_and_validate_plugins(
                        site_name, site_config
                    )
                    if plugin_instances is None:
                        logger.error(
                            f"Failed to load plugins for site '{site_name}'. Skipping."
                        )
                        return

                    fetcher, parser, transformer, storage_plugins = plugin_instances

                    # Now call _scrape_website with all required arguments
                    await self._scrape_website(
                        site_name=site_name,
                        site_config=site_config,
                        fetcher=fetcher,
                        parser=parser,
                        transformer=transformer,
                        storage_plugins=storage_plugins,
                    )
                except Exception as e:
                    logger.error(
                        f"Unhandled error during processing of site '{site_name}': {e}",
                        exc_info=True,
                    )
                finally:
                    logger.debug(f"Released semaphore for site: {site_name}")

        for site_name, site_config in websites.items():
            if not site_config.get("enabled", True):
                logger.info(f"Website '{site_name}' is disabled. Skipping.")
                continue
            website_tasks.append(throttled_scrape(site_name, site_config))

        if website_tasks:
            await asyncio.gather(*website_tasks)

        logger.info("Orchestrator finished.")

    async def cleanup(self):
        """Cleanup resources and close connections."""
        if self.storage_plugins:
            close_tasks = []
            for storage in self.storage_plugins:
                if hasattr(storage, "close") and callable(storage.close):
                    close_tasks.append(asyncio.create_task(storage.close()))

            if close_tasks:
                await asyncio.gather(*close_tasks, return_exceptions=True)
                logger.info("Closed all storage connections")
