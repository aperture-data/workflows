from fastmcp.server.auth import BearerAuthProvider
import os
import asyncio
import functools

from fastmcp import FastMCP

from decorators import register_tools, register_resources
import tools
import resources
from shared import logger
from shared import args

from status_tools import StatusUpdater, WorkflowStatus


def test_connection():
    """Test connection to the ApertureDB database."""
    from aperturedb.CommonLibrary import create_connector
    client = create_connector()
    from aperturedb.Utils import Utils
    utils = Utils(client)
    utils.get_schema()


test_connection()


mcp = FastMCP(
    name="ApertureDB",
    instructions="""
    This  MCP server providers access to an instance of the ApertureDB database.
    This is a multi-modal hybrid graph and vector database.
    It supports both vector and graph queries, allowing you to perform complex data retrieval and analysis.
    """,
)

register_tools(mcp) 
register_resources(mcp)

updater = StatusUpdater()
updater.post_update(
    status=WorkflowStatus.RUNNING,
    accessible=True,
)