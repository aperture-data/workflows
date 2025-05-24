from typing import Callable, List, Tuple
from pydantic import BaseModel
from mcp.server.fastmcp import FastMCP
from fastapi import APIRouter, Depends, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import inspect
import os

_registered_tools: List[Tuple[str, Callable]] = []

bearer_scheme = HTTPBearer(auto_error=False)


def require_token_factory(token: str):
    def require_token(auth: HTTPAuthorizationCredentials = Security(bearer_scheme)):
        if not auth or auth.credentials != token:
            # can be wrapped in HTTPException later
            raise ValueError("Invalid or missing token")
    return require_token


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


def register_tools(mcp: FastMCP, app: APIRouter, token: str):
    """Registers all tools with both MCP and API"""
    from fastapi import HTTPException

    token_validator = require_token_factory(token)

    for tool_name, fn, input_type, output_type in _registered_tools:
        def make_route(fn=fn, input_type=input_type, output_type=output_type):
            def route(req: input_type, token=Depends(token_validator)) -> output_type:
                try:
                    return fn(req)
                except Exception as e:
                    raise HTTPException(status_code=500, detail=str(e))
            return route

        app.post(
            f"/debug/{tool_name}",
            response_model=output_type,
            name=f"debug_{tool_name}",
            description=fn.__doc__ or f"Debug route for {tool_name}",
        )(make_route())

        @mcp.tool(name=tool_name, description=fn.__doc__ or "")
        def mcp_wrapper(req: input_type) -> output_type:
            return fn(req)
