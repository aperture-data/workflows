from typing import Callable, List, Tuple
from pydantic import BaseModel
# from fastmcp.server.tool import Tool
# from mcp.server.fastmcp import FastMCP
# from fastapi import APIRouter, Depends, Security
# from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import inspect
import os
from shared import logger

_registered_tools: List[Tuple[str, Callable]] = []


def declare_mcp_tool(fn=None, *, name: str = None):
    """Decorator to expose a tool to both MCP and FastAPI, DRY."""
    def decorator(fn: Callable[[BaseModel], BaseModel]):
        # sig = inspect.signature(fn)
        # if len(sig.parameters) != 1:
        #     raise ValueError(
        #         "Tool functions must take a single Pydantic model argument")

        # input_type = list(sig.parameters.values())[0].annotation
        # output_type = sig.return_annotation
        tool_name = name or fn.__name__

        # Save for FastAPI registration
        _registered_tools.append((tool_name, fn))
        # logger.info(
        #     f"Registered tool: {tool_name} with input type {input_type} and output type {output_type}; we now have {_registered_tools} tools registered")
        return fn
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

        # if issubclass(input_type, BaseModel):
        #     input_schema = input_type.schema()
        #     params = {
        #         "type": "object",
        #         "properties": input_schema["properties"],
        #         "required": input_schema.get("required", []),
        #     }
        # else:
        #     input_schema = input_type

        # if issubclass(output_type, BaseModel):
        #     output_schema = output_type.schema()
        #     response = {
        #         "type": "object",
        #         "properties": output_schema["properties"],
        #         "required": output_schema.get("required", []),
        #     }
        # else:
        #     response = output_type

        # mcp.tool(name=tool_name, description=fn.__doc__ or "",
        #          parameters=params, response=response, handler=fn)

        mcp.tool(fn, name=tool_name, description=fn.__doc__ or "",)
