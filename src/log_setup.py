import json
import logging.config
import os
from logging import DEBUG
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

LOG_CONFIGURATION_FILENAME = 'configurations/log-configuration.json'
abs_path = str(Path(Path(__file__).resolve().parents[1], LOG_CONFIGURATION_FILENAME))


def setup_logging(
    default_path=abs_path,
    default_level=logging.INFO,
    env_key='LOG_CFG'
):
    """Setup logging configuration

    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.isfile(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
        print("Successfully loaded logging configuration.")
    else:
        logging.basicConfig(level=default_level)
        print("Failed to load logging configuration, use basic config.")


class DebugFileHandler(TimedRotatingFileHandler):
    def __init__(self, filename, when='h', interval=1, backupCount=0, encoding=None, delay=False, utc=False, atTime=None):
        TimedRotatingFileHandler.__init__(self, filename, when, interval, backupCount, encoding, delay, utc, atTime)

    def emit(self, record):
        if not record.levelno == DEBUG:
            return
        TimedRotatingFileHandler.emit(self, record)
