"""
Import all transformers here to make them available to the plugin registry.

This allows for dynamic loading of transformers based on configuration.
"""

from scraper_system.plugins.transformers.eljannah_transformer import EljannahTransformer
from scraper_system.plugins.transformers.grilld_transformer import GrilldTransformer
from scraper_system.plugins.transformers.gyg_transformer import GygTransformer
from scraper_system.plugins.transformers.noodlebox_transformer import NoodleboxTransformer

# Add transformer class registry
TRANSFORMER_REGISTRY = {
    'eljannah': EljannahTransformer,
    'grilld': GrilldTransformer,
    'gyg': GygTransformer,
    'noodlebox': NoodleboxTransformer,
}
