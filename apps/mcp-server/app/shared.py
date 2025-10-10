import logging
import os

from connection_pool import ConnectionPool


def configure_logging(log_level):
    logger = logging.getLogger("aperture")  # use a named logger
    logger.setLevel(log_level.upper())

    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%SZ"
        ))
        logger.addHandler(handler)

    logger.info(f"Logger configured with level: {log_level}")
    return logger


def get_args():
    from wf_argparse import ArgumentParser
    parser = ArgumentParser(
        description="ApertureDB MCP server")
    parser.add_argument("--input", required=False,
                        help="Default descriptor set to use for find similar")
    parser.add_argument("--auth-token", required=True, type=str,
                        help="Bearer token for authentication")
    parser.add_argument("--log-level", type=str, default='DEBUG',
                        help="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)")

    args = parser.parse_args([])  # suppress command line parsing
    return args


args = get_args()
logger = configure_logging(args.log_level)
connection_pool = ConnectionPool()
