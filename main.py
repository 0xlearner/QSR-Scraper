import asyncio
import logging
import yaml # PyYAML
from scraper_system.core.orchestrator import Orchestrator
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

CONFIG_PATH = "configs/config.yaml"
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

def setup_logging(log_level_str: str = "INFO"):
    """Configures basic logging."""
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)
    logging.basicConfig(level=log_level, format=LOG_FORMAT)
    # Optionally suppress verbose logs from libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)


def load_config(path: str) -> dict:
    """Loads YAML configuration file and processes environment variables."""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            if config is None:
                logging.error(f"Configuration file {path} is empty or invalid.")
                return {}

            # Process the loaded config to replace env var placeholders
            config = process_env_vars(config)
            return config
    except FileNotFoundError:
        logging.error(f"Configuration file not found at {path}")
        return {}
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML configuration file {path}: {e}")
        return {}
    except Exception as e:
        logging.error(f"Failed to load config from {path}: {e}")
        return {}

def process_env_vars(item):
    """Recursively process dictionary values to replace ${ENV_VAR} patterns with actual env values."""
    if isinstance(item, dict):
        return {k: process_env_vars(v) for k, v in item.items()}
    elif isinstance(item, list):
        return [process_env_vars(i) for i in item]
    elif isinstance(item, str) and item.startswith("${") and item.endswith("}"):
        # Extract the environment variable name
        env_var = item[2:-1]
        # Get the environment variable value, with optional default
        if ":" in env_var:
            env_var, default = env_var.split(":", 1)
            return os.environ.get(env_var, default)
        return os.environ.get(env_var, f"ENV_{env_var}_NOT_FOUND")
    return item

async def main():
    """Main execution function."""
    config = load_config(CONFIG_PATH)
    if not config:
        print(f"Exiting due to configuration loading errors. Check {CONFIG_PATH}")
        return

    log_level = config.get("global_settings", {}).get("log_level", "INFO")
    setup_logging(log_level)

    # Ensure data directory exists if specified in any storage config
    # A more robust approach would check paths within the storage plugins themselves
    # or dynamically based on configured paths. For simplicity now, check a default.
    if not os.path.exists("data"):
        try:
            os.makedirs("data")
            logging.info("Created data output directory.")
        except OSError as e:
            logging.error(f"Failed to create data directory: {e}")
            # Decide if this is critical and should exit

    orchestrator = Orchestrator(config)
    await orchestrator.run()

if __name__ == "__main__":
    asyncio.run(main())
