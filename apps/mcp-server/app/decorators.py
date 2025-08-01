from typing import Callable, List, Tuple
from pydantic import BaseModel
from fastapi import HTTPException
import inspect
import os
import traceback
import functools

from shared import logger, args
from fastmcp.server.dependencies import get_http_headers
from fastmcp.exceptions import ToolError, ResourceError
from dataclasses import dataclass


@dataclass
class Tool:
    """A class to represent a tool in MCP."""
    function: Callable

    @property
    def name(self) -> str:
        """Get the name of the tool."""
        if hasattr(self.function, "__name__"):
            return self.function.__name__
        return self.function.__class__.__name__

    @property
    def description(self) -> str:
        """Get the description of the tool."""
        if self.function.__doc__:
            return self.function.__doc__.strip()
        return ""

    def get_function(self) -> Callable:
        @functools.wraps(self.function)
        def wrapper(*args, **kwargs):
            """Wrapper to call the tool function."""
            logger.info(
                f"Calling tool: {self.name} with args: {args}, kwargs: {kwargs}")
            check_auth()

            # Ordinary exceptions may not be propagated to the client, so we catch them here and re-wrap them
            # as ToolError with an informative message.
            # Note that this potentially leaks sensitive information about the implementation.
            try:
                return self.function(*args, **kwargs)
            except Exception as e:
                logger.exception(f"Tool {self.name} crashed")
                raise ToolError(
                    f"Internal error in {self.name}: {type(e).__name__}: {str(e)} - {traceback.format_exc()}")

        return wrapper


@dataclass
class Resource():
    """A class to represent a resource in MCP."""
    uri: str
    function: Callable

    @property
    def full_uri(self) -> str:
        """Get the full URL of the resource."""
        return f"aperturedb:/{self.uri}"

    @property
    def name(self) -> str:
        """Get the name of the resource."""
        if hasattr(self.function, "__name__"):
            return self.function.__name__
        return self.function.__class__.__name__

    @property
    def description(self) -> str:
        """Get the description of the resource."""
        if self.function.__doc__:
            return self.function.__doc__.strip()
        return ""

    def get_function(self) -> Callable:
        @functools.wraps(self.function)
        def wrapper(*args, **kwargs):
            """Wrapper to call the resource function."""
            logger.info(
                f"Calling resource: {self.name} with args: {args}, kwargs: {kwargs}")
            check_auth()

            # Ordinary exceptions may not be propagated to the client, so we catch them here and re-wrap them
            # as ToolError with an informative message.
            # Note that this potentially leaks sensitive information about the implementation.
            try:
                return self.function(*args, **kwargs)
            except Exception as e:
                logger.exception(f"Resource {self.name} crashed")
                raise ResourceError(
                    f"Internal error in {self.name}: {type(e).__name__}: {str(e)} - {traceback.format_exc()}")

        return wrapper


_registered_tools: List[Tool] = []
_registered_resources: List[Resource] = []
# _registered_prompts: List[Tuple[str, Callable]] = []

TOKEN = args.auth_token
assert TOKEN, "You must provide a valid auth token in WF_AUTH_TOKEN"


def check_auth():
    """Check if the request has a valid bearer token."""
    logger.debug("Checking authentication")
    headers = get_http_headers() or {}
    auth = headers.get("authorization", "")
    if not auth or auth != f"Bearer {TOKEN}":
        logger.error("Invalid bearer token")
        raise HTTPException(status_code=401, detail="Invalid bearer token")


def declare_mcp_tool(fn=None):
    """Decorator to expose a tool to both MCP and FastAPI, DRY."""
    def decorator(fn: Callable):
        tool = Tool(function=fn)
        _registered_tools.append(tool)
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

    for tool in _registered_tools:
        logger.info(f"Registering tool: {tool.name}")

        # Register the tool with MCP
        mcp.tool(
            tool.get_function(),
            name=tool.name,
            description=tool.description,
        )


def declare_mcp_resource(uri: str, fn=None):
    """Decorator to expose a resource to both MCP and FastAPI, DRY."""
    def decorator(fn: Callable):
        resource = Resource(uri=uri, function=fn)
        _registered_resources.append(resource)
        return fn

    # Support for both styles of decorator usage
    if fn is None:
        return decorator
    else:
        return decorator(fn)


def register_resources(mcp: "FastMCP"):
    """Registers all resources with MCP"""
    from fastapi import HTTPException
    logger.info(
        f"Registering {len(_registered_resources)} resources with {mcp}")

    for resource in _registered_resources:
        logger.info(f"Registering resource: {resource.name}")

        # Register the resource with MCP
        mcp.resource(
            uri=resource.full_uri,
            name=resource.name,
            description=resource.description,
        )(fn=resource.get_function())
