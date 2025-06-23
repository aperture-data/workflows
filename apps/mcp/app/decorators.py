from typing import Callable, List, Tuple
from pydantic import BaseModel
# from mcp.server.fastmcp import FastMCP
# from fastapi import APIRouter, Depends, Security
# from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import inspect
import os

_registered_tools: List[Tuple[str, Callable]] = []


def declare_mcp_tool(name: str = None):
    """Decorator to expose a tool to both MCP and FastAPI, DRY."""
    def decorator(fn: Callable[[BaseModel], BaseModel]):
        sig = inspect.signature(fn)
        if len(sig.parameters) != 1:
            raise ValueError(
                "Tool functions must take a single Pydantic model argument")

        input_type = list(sig.parameters.values())[0].annotation
        output_type = sig.return_annotation
        tool_name = name or fn.__name__

        # Save for FastAPI registration
        _registered_tools.append((tool_name, fn, input_type, output_type))
        return fn
    return decorator


def register_tools(mcp: "FastMCP"):
    """Registers all tools with MCP"""
    from fastapi import HTTPException

    for tool_name, fn, input_type, output_type in _registered_tools:
        @mcp.tool(name=tool_name, description=fn.__doc__ or "")
        def mcp_wrapper(req: input_type) -> output_type:
            return fn(req)
