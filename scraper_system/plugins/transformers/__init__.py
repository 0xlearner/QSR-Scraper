"""
Import all transformers here to make them available to the plugin registry.

This allows for dynamic loading of transformers based on configuration.
"""

from scraper_system.plugins.transformers.eljannah_transformer import EljannahTransformer
from scraper_system.plugins.transformers.grilld_transformer import GrilldTransformer
from scraper_system.plugins.transformers.gyg_transformer import GygTransformer
from scraper_system.plugins.transformers.nandos_transformer import NandosTransformer
from scraper_system.plugins.transformers.noodlebox_transformer import (
    NoodleboxTransformer,
)
from scraper_system.plugins.transformers.oporto_transformer import (
    OportoTransformer,
)
from scraper_system.plugins.transformers.redrooster_transformer import (
    RedRoosterTransformer,
)
from scraper_system.plugins.transformers.zambrero_transformer import ZambreroTransformer
from scraper_system.plugins.transformers.zeus_transformer import ZeusTransformer

# Add transformer class registry
TRANSFORMER_REGISTRY = {
    "eljannah": EljannahTransformer,
    "grilld": GrilldTransformer,
    "gyg": GygTransformer,
    "nandos": NandosTransformer,
    "noodlebox": NoodleboxTransformer,
    "oporto": OportoTransformer,
    "redrooster": RedRoosterTransformer,
    "zambrero": ZambreroTransformer,
    "zeus": ZeusTransformer,
}
