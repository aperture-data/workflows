"""Workflow parameters.

The admin token is read straight from the environment. wf_argparse logs the
values it validates, including the defaults it takes from the environment for
hidden arguments, and the token must not appear in the workflow logs.
"""

import logging
import os

from aperturedb_workflows.wf_argparse import ArgumentParser

BASE_PATH = "/nexus"
ADMIN_PATH = f"{BASE_PATH}/admin"
MCP_PATH = f"{BASE_PATH}/mcp/"
ADMIN_TOKEN_ENVAR = "WF_ADMIN_TOKEN"


def _parse_args():
    parser = ArgumentParser(description="Aperture Nexus memory server")
    parser.add_argument("--log-level", type="log_level", default="WARNING")
    parser.envars_used.add(ADMIN_TOKEN_ENVAR)
    return parser.parse_args([])


args = _parse_args()
admin_token = os.environ.get(ADMIN_TOKEN_ENVAR, "").strip()
if not admin_token:
    raise SystemExit(f"{ADMIN_TOKEN_ENVAR} must be set")


def configure_logging() -> logging.Logger:
    logger = logging.getLogger("aperture")
    logger.setLevel(args.log_level)
    logger.propagate = False  # wf_argparse may already have given the root logger a handler
    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%SZ"))
        logger.addHandler(handler)
    return logger
