import asyncio
import logging
import yaml  # PyYAML
from scraper_system.core.orchestrator import Orchestrator
import os
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

# Load environment variables from .env file
load_dotenv()

CONFIG_PATH = "configs/config.yaml"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_DIR = "logs"  # Directory to store log files


def setup_logging(log_level_str: str = "INFO", log_to_file: bool = True):
    """
    Configures logging to both console and file.

    Args:
        log_level_str: Logging level as string (DEBUG, INFO, WARNING, ERROR)
        log_to_file: Whether to also log to a file
    """
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)

    # Create root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Remove existing handlers to avoid duplicate logs
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter(LOG_FORMAT)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # Create file handler if requested
    if log_to_file:
        # Create logs directory if it doesn't exist
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR)

        # Create rotating file handler (10 MB max size, keep 5 backup files)
        log_file_path = os.path.join(LOG_DIR, "scraper.log")
        file_handler = RotatingFileHandler(
            log_file_path, maxBytes=10 * 1024 * 1024, backupCount=5  # 10 MB
        )
        file_handler.setLevel(log_level)
        file_formatter = logging.Formatter(LOG_FORMAT)
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

        logging.info(f"Logging to file: {log_file_path}")

    # Optionally suppress verbose logs from libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)


def load_config(path: str) -> dict:
    """Loads YAML configuration file and processes environment variables."""
    try:
        with open(path, "r", encoding="utf-8") as f:
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

    # Get logging configuration from config
    log_level = config.get("global_settings", {}).get("log_level", "INFO")
    log_to_file = config.get("global_settings", {}).get("log_to_file", True)

    # Setup logging with file output
    setup_logging(log_level, log_to_file)

    logging.info("=== QSR Scraper System Starting ===")

    # Ensure data directory exists if specified in any storage config
    if not os.path.exists("data"):
        try:
            os.makedirs("data")
            logging.info("Created data output directory.")
        except OSError as e:
            logging.error(f"Failed to create data directory: {e}")
            # Decide if this is critical and should exit

    orchestrator = Orchestrator(config)
    await orchestrator.run()

    logging.info("=== QSR Scraper System Finished ===")


if __name__ == "__main__":
    asyncio.run(main())
