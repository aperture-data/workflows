"""Helpers that drive the portal and the MCP endpoint over HTTP."""

import os
import re
import uuid

import httpx
import pytest
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport

BASE_URL = os.environ.get("NEXUS_URL", "http://nexus:8000")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "test-admin-token")
ADMIN = "/nexus/admin"
MCP_URL = f"{BASE_URL}/nexus/mcp/"


def unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class Portal:
    """An admin browser session against the portal."""

    def __init__(self):
        self.http = httpx.Client(base_url=BASE_URL, follow_redirects=False, timeout=120)

    def login(self) -> httpx.Response:
        response = self.http.get(f"{ADMIN}/login", params={"token": ADMIN_TOKEN})
        assert response.status_code == 303, response.text
        return response

    def page(self) -> httpx.Response:
        response = self.http.get(f"{ADMIN}/")
        assert response.status_code == 200, response.text
        return response

    def csrf(self) -> str:
        return re.search(r'name="csrf" value="([^"]+)"', self.page().text).group(1)

    def post(self, path: str, **data) -> httpx.Response:
        return self.http.post(f"{ADMIN}{path}", data={"csrf": self.csrf(), **data})

    @staticmethod
    def issued_token(response: httpx.Response) -> str:
        assert response.status_code == 200, response.text
        return re.search(r"data-agent-token>([^<]+)<", response.text).group(1)

    def add_agent(self, agent_id: str, **fields) -> str:
        return self.issued_token(self.post("/agents", agent_id=agent_id, **fields))

    def rotate(self, agent_id: str) -> str:
        return self.issued_token(self.post("/agents/rotate", agent_id=agent_id))

    def remove(self, agent_id: str) -> None:
        response = self.post("/agents/delete", agent_id=agent_id)
        assert response.status_code == 200, response.text


@pytest.fixture(scope="session")
def portal() -> Portal:
    session = Portal()
    session.login()
    return session


def mcp_client(token: str) -> Client:
    transport = StreamableHttpTransport(MCP_URL, headers={"Authorization": f"Bearer {token}"})
    return Client(transport, timeout=120)


async def call(token: str, tool: str, **arguments):
    """Call a tool and return its result as plain JSON values.

    fastmcp wraps results that are not objects as {"result": ...}.
    """
    async with mcp_client(token) as client:
        result = await client.call_tool(tool, arguments)
        data = result.structured_content
        if isinstance(data, dict) and list(data) == ["result"]:
            return data["result"]
        return data


def initialize(token: str = None, authorization: str = None) -> httpx.Response:
    """Send a raw MCP initialize request with a bearer token or a raw Authorization header."""
    headers = {"Accept": "application/json, text/event-stream"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    if authorization is not None:
        headers["Authorization"] = authorization
    return httpx.post(MCP_URL, headers=headers, timeout=60, json={
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                   "clientInfo": {"name": "nexus-tests", "version": "1"}},
    })
