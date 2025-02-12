from os import getenv
import yaml
import logging
import logging.config

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "detailed": {
            "format": "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "simple": {
            "format": "%(levelname)s: %(message)s",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "detailed",
            "level": "DEBUG",
        }
    },
    "root": {"level": "DEBUG", "handlers": ["console"]},
}

logging_config = LOGGING_CONFIG
logging_config_file = getenv("LOGGING_CONFIG_FILE")
if logging_config_file:
    with open(logging_config_file, "r") as fp:
        logging_config = yaml.safe_load(fp)
logging.config.dictConfig(logging_config)
