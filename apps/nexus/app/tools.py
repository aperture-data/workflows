"""MCP endpoint: agent authentication and memory tools."""

import logging
from typing import Annotated, Optional, Union

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.dependencies import get_http_request
from pydantic import Field
from starlette.responses import JSONResponse

from aperture_nexus.exceptions import NexusPermissionError, NexusValidationError
from service import AccessError, NexusService

logger = logging.getLogger("aperture.nexus.mcp")

PRINCIPAL_KEY = "nexus_principal"

INSTRUCTIONS = """\
Aperture Nexus is long-term memory shared by this organization's agents.
Call `recall` before starting a task to see what is already known about it.
Call `remember` to store decisions, findings, outcomes and open questions,
one self-contained statement per call, under a session named after the task or
project and with a purpose that says why the work is happening.
"""


class AgentAuthMiddleware:
    """Authenticate every MCP request with its agent's bearer token.

    Authentication runs on each request, so removing or rotating an agent in
    the portal takes effect immediately.
    """

    def __init__(self, app, nexus: NexusService):
        self.app = app
        self.nexus = nexus

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        header = dict(scope.get("headers") or []).get(b"authorization", b"").decode("latin-1")
        scheme, _, token = header.partition(" ")
        principal = None
        if scheme.lower() == "bearer" and token.strip():
            principal = await self.nexus.call(self.nexus.authenticate, token.strip())
        if principal is None:
            response = JSONResponse(
                {"error": "invalid_token",
                 "error_description": "Use an agent token from the Nexus portal"},
                status_code=401,
                headers={"WWW-Authenticate": 'Bearer realm="nexus"'})
            await response(scope, receive, send)
            return
        scope.setdefault("state", {})[PRINCIPAL_KEY] = principal
        await self.app(scope, receive, send)


def _principal():
    principal = getattr(get_http_request().state, PRINCIPAL_KEY, None)
    if principal is None:
        raise ToolError("Not authenticated")
    return principal


async def _run(nexus: NexusService, name: str, fn, **kwargs):
    principal = _principal()
    try:
        return await nexus.call(fn, principal, **kwargs)
    except (AccessError, NexusValidationError, NexusPermissionError) as e:
        raise ToolError(str(e)) from e
    except Exception as e:
        logger.exception("Tool %s failed for agent %s", name, principal.user_id)
        raise ToolError(f"{name} failed: {type(e).__name__}") from e


Text = Annotated[str, Field(description="What to remember: one self-contained statement")]
Session = Annotated[str, Field(description="Task or project name; reuse it to group related memories")]
OptionalText = Optional[str]


def register_tools(mcp: FastMCP, nexus: NexusService) -> None:

    @mcp.tool
    async def remember(
        text: Text,
        session: Session,
        purpose: Annotated[OptionalText, Field(description="Why this work is happening")] = None,
        metadata: Annotated[Optional[dict[str, Union[str, int, float, bool]]],
                            Field(description="Extra searchable properties")] = None,
    ) -> dict:
        """Store a memory for this agent. Returns its commit_id and context_id."""
        return await _run(nexus, "remember", nexus.remember, text=text,
                          session=session, purpose=purpose, metadata=metadata)

    @mcp.tool
    async def recall(
        query: Annotated[OptionalText, Field(description="What to look for; omit to list by filters only")] = None,
        session: Annotated[OptionalText, Field(description="Only memories from this session")] = None,
        agent: Annotated[OptionalText, Field(description="Only memories stored by this agent ID")] = None,
        department: Annotated[OptionalText, Field(description="Only memories from this department")] = None,
        k: Annotated[int, Field(ge=1, le=50, description="Maximum number of results")] = 10,
    ) -> list[dict]:
        """Search memories stored by agents in this organization, most relevant first."""
        return await _run(nexus, "recall", nexus.recall, query=query, session=session,
                          agent=agent, department=department, k=k)

    @mcp.tool
    async def find_sessions(
        query: Annotated[str, Field(description="Describe the work you are looking for")],
        agent: Annotated[OptionalText, Field(description="Only sessions of this agent ID")] = None,
        department: Annotated[OptionalText, Field(description="Only sessions from this department")] = None,
        k: Annotated[int, Field(ge=1, le=50, description="Maximum number of contexts")] = 10,
    ) -> list[dict]:
        """Find earlier sessions by the purpose of their work."""
        return await _run(nexus, "find_sessions", nexus.find_sessions, query=query,
                          agent=agent, department=department, k=k)

    @mcp.tool
    async def get_context(
        context_id: Annotated[str, Field(description="A context_id from remember or recall")],
    ) -> dict:
        """Return everything stored under one context, with who stored it and why."""
        return await _run(nexus, "get_context", nexus.get_context, context_id=context_id)

    @mcp.tool
    async def link(
        source_context_id: Annotated[str, Field(description="A context this agent stored")],
        target_context_id: Annotated[str, Field(description="A related context in this organization")],
        relationship: Annotated[str, Field(description="For example follows, caused_by or references")],
    ) -> dict:
        """Record how one of this agent's contexts relates to another context."""
        return await _run(nexus, "link", nexus.link, source_context_id=source_context_id,
                          target_context_id=target_context_id, relationship=relationship)

    @mcp.tool
    async def forget(
        commit_id: Annotated[OptionalText, Field(description="Remove one remember call")] = None,
        context_id: Annotated[OptionalText, Field(description="Remove a whole context")] = None,
        session: Annotated[OptionalText, Field(description="Remove a whole session of this agent")] = None,
    ) -> dict:
        """Delete memories this agent stored. Pass exactly one argument."""
        return await _run(nexus, "forget", nexus.forget, commit_id=commit_id,
                          context_id=context_id, session=session)
