from typing import Type, Dict, Any, Optional, List
import importlib
import logging
from scraper_system.interfaces.fetcher_interface import FetcherInterface
from scraper_system.interfaces.parser_interface import ParserInterface
from scraper_system.interfaces.transformer_interface import TransformerInterface
from scraper_system.interfaces.storage_interface import StorageInterface

logger = logging.getLogger(__name__)


class PluginFactory:
    """Factory class for creating and managing scraper plugins"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._plugin_paths = {
            "fetcher": "scraper_system.plugins.fetchers",
            "parser": "scraper_system.plugins.parsers",
            "transformer": "scraper_system.plugins.transformers",
            "storage": "scraper_system.plugins.storage",
        }

    def _import_plugin_class(
        self, plugin_name: str, plugin_type: str
    ) -> Optional[Type]:
        """Import a plugin class based on its name and type"""
        try:
            # Determine the module path based on plugin type and name
            base_path = self._plugin_paths.get(plugin_type)
            if not base_path:
                raise ValueError(f"Unknown plugin type: {plugin_type}")

            # Convert plugin name to expected module name (e.g., AsyncHTTPXFetcher -> http_fetcher)
            module_name = self._convert_class_to_module_name(plugin_name)
            full_module_path = f"{base_path}.{module_name}"

            # Import the module and get the class
            module = importlib.import_module(full_module_path)
            plugin_class = getattr(module, plugin_name)

            return plugin_class

        except (ImportError, AttributeError) as e:
            logger.error(f"Failed to import plugin {plugin_name}: {e}")
            return None

    def _convert_class_to_module_name(self, class_name: str) -> str:
        """Convert CamelCase class name to snake_case module name"""
        # Special handling for known cases
        special_cases = {
            "AsyncHTTPXFetcher": "http_fetcher",
            "ZendriverFetcher": "zendriver_fetcher",
            "GYGParser": "gyg_parser",
            "ChargrillCharliesParser": "chargrillcharlies_parser",
            "ChargrillCharliesTransformer": "chargrillcharlies_transformer",
            "YochiParser": "yochi_parser",
            "YochiTransformer": "yochi_transformer",
            "NandosParser": "nandos_parser",
            "NandosTransformer": "nandos_transformer",
            "OportoParser": "oporto_parser",
            "OportoTransformer": "oporto_transformer",
            "RedRoosterParser": "redrooster_parser",
            "RedRoosterTransformer": "redrooster_transformer",
            "ZambreroParser": "zambrero_parser",
            "ZambreroTransformer": "zambrero_transformer",
            "ZeusParser": "zeus_parser",
            "ZeusTransformer": "zeus_transformer",
            "JSONStorage": "json_storage",
        }

        if class_name in special_cases:
            return special_cases[class_name]

        # Generic conversion for other cases
        import re

        # Convert camel case to snake case
        name = re.sub("([A-Z])", r"_\1", class_name).lower().lstrip("_")
        return name

    def create_fetcher(self, site_config: Dict[str, Any]) -> Optional[FetcherInterface]:
        """Create a fetcher instance based on site configuration"""
        fetcher_name = site_config.get("fetcher")
        if not fetcher_name:
            return None

        fetcher_class = self._import_plugin_class(fetcher_name, "fetcher")
        if not fetcher_class:
            return None

        # Pass the entire fetcher_options as a single config dictionary
        fetcher_config = site_config.get("config", {}).get("fetcher_options", {})
        try:
            return fetcher_class(config=fetcher_config)  # Pass as named parameter
        except Exception as e:
            logger.error(f"Failed to instantiate fetcher {fetcher_name}: {e}")
            return None

    def create_parser(
        self, site_config: Dict[str, Any], fetcher: FetcherInterface
    ) -> Optional[ParserInterface]:
        """Create a parser instance based on site configuration"""
        parser_name = site_config.get("parser")
        if not parser_name:
            return None

        parser_class = self._import_plugin_class(parser_name, "parser")
        if not parser_class:
            return None

        try:
            return parser_class(fetcher=fetcher)
        except Exception as e:
            logger.error(f"Failed to instantiate parser {parser_name}: {e}")
            return None

    def create_transformer(
        self, site_config: Dict[str, Any]
    ) -> Optional[TransformerInterface]:
        """Create a transformer instance based on site configuration"""
        transformer_name = site_config.get("transformer")
        if not transformer_name:
            return None

        transformer_class = self._import_plugin_class(transformer_name, "transformer")
        if not transformer_class:
            return None

        transformer_config = site_config.get("config", {}).get(
            "transformer_options", {}
        )
        try:
            if "api_key" in transformer_config:
                return transformer_class(api_key=transformer_config["api_key"])
            return transformer_class()
        except Exception as e:
            logger.error(f"Failed to instantiate transformer {transformer_name}: {e}")
            return None

    def create_storage_plugins(
        self, site_config: Dict[str, Any]
    ) -> List[StorageInterface]:
        """Create storage plugin instances based on site configuration"""
        storage_names = site_config.get("storage", [])
        storage_plugins = []

        for storage_name in storage_names:
            storage_class = self._import_plugin_class(storage_name, "storage")
            if storage_class:
                try:
                    storage_plugins.append(storage_class())
                except Exception as e:
                    logger.error(
                        f"Failed to instantiate storage plugin {storage_name}: {e}"
                    )

        return storage_plugins

    def create_plugins_for_site(self, site_name: str, site_config: Dict[str, Any]):
        """Create all plugins needed for a specific site"""
        fetcher = self.create_fetcher(site_config)
        if not fetcher:
            raise ValueError(f"Failed to create fetcher for site {site_name}")

        parser = self.create_parser(site_config, fetcher)
        if not parser:
            raise ValueError(f"Failed to create parser for site {site_name}")

        transformer = self.create_transformer(site_config)
        storage_plugins = self.create_storage_plugins(site_config)

        return fetcher, parser, transformer, storage_plugins
