from typing import Callable, List, Tuple
from pydantic import BaseModel
from fastapi import HTTPException
import inspect
import os
import functools

from shared import logger, args
from fastapi import Depends
from fastmcp.server.dependencies import get_http_headers

_registered_tools: List[Tuple[str, Callable]] = []

TOKEN = args.auth_token
assert TOKEN, "You must provide a valid auth token in WF_AUTH_TOKEN"


def check_auth():
    """Check if the request has a valid bearer token."""
    headers = get_http_headers() or {}
    auth = headers.get("authorization", "")
    if not auth or auth != f"Bearer {TOKEN}":
        logger.error("Invalid bearer token")
        raise HTTPException(status_code=401, detail="Invalid bearer token")


def declare_mcp_tool(fn=None, *, name: str = None):
    """Decorator to expose a tool to both MCP and FastAPI, DRY."""
    def decorator(fn: Callable[[BaseModel], BaseModel]):
        tool_name = name or fn.__name__

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            """Wrapper to call the tool function."""
            logger.info(
                f"Calling tool: {tool_name} with args: {args}, kwargs: {kwargs}")
            check_auth()
            return fn(*args, **kwargs)

        _registered_tools.append((tool_name, wrapper))
        return fn

    # Support for both styles of decorator usage
    if fn is None:
        return decorator
    else:
        return decorator(fn)


def register_tools(mcp: "FastMCP"):
    """Registers all tools with MCP"""
    from fastapi import HTTPException
    logger.info(f"Registering {len(_registered_tools)} tools with {mcp}")

    for tool_name, fn in _registered_tools:
        logger.info(f"Registering tool: {tool_name}")

        # Register the tool with MCP
        mcp.tool(
            fn,
            name=tool_name,
            description=fn.__doc__ or "",
        )
