import logging
import os

logger = logging.getLogger("aperture")  # use a named logger
LOG_LEVEL = os.getenv('WF_LOG_LEVEL', 'INFO').upper()
logger.setLevel(LOG_LEVEL)

if not logger.hasHandlers():
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ"
    ))
    logger.addHandler(handler)

logger.info("Logger configured!")
