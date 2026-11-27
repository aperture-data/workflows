"""Aperture Nexus workflow server.

Everything is served under /nexus/ on port 8000, the workflow's one ingress:
    /nexus/         redirects to the admin portal
    /nexus/admin/   admin portal (agents, connection details)
    /nexus/mcp/     MCP endpoint for agents (streamable HTTP, agent bearer token)
    /nexus/health   readiness check
"""

# settings must be imported first: see its docstring.
from settings import ADMIN_PATH, BASE_PATH, admin_token, configure_logging  # isort: skip

import contextlib
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastmcp import FastMCP
from starlette.middleware import Middleware

from portal import create_router
from service import NexusService
from aperturedb_workflows.status_tools import StatusUpdater, WorkflowStatus
from tools import INSTRUCTIONS, AgentAuthMiddleware, register_tools

logger = configure_logging()

nexus = NexusService()

mcp = FastMCP(name="Aperture Nexus", instructions=INSTRUCTIONS)
register_tools(mcp, nexus)
mcp_app = mcp.http_app(
    path="/mcp/",
    stateless_http=True,
    middleware=[Middleware(AgentAuthMiddleware, nexus=nexus)],
)

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Preparing Nexus schema and embedding model %s", nexus.text_model)
    await nexus.call(nexus.prepare)
    async with mcp_app.lifespan(app):
        StatusUpdater().post_update(status=WorkflowStatus.RUNNING, accessible=True)
        logger.info("Aperture Nexus is serving under %s/", BASE_PATH)
        yield


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)


@app.get(f"{BASE_PATH}/health")
async def health():
    """Uvicorn serves requests only after startup finished, so reaching this means ready."""
    return {"status": "ok"}


@app.get(BASE_PATH)
@app.get(f"{BASE_PATH}/")
async def root():
    return RedirectResponse(f"{ADMIN_PATH}/", status_code=303)


@app.get(ADMIN_PATH)
async def admin_without_slash():
    return RedirectResponse(f"{ADMIN_PATH}/", status_code=303)


app.include_router(create_router(nexus, admin_token), prefix=ADMIN_PATH)
app.mount(f"{ADMIN_PATH}/static",
          StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")
# Mounted last: /nexus/admin routes above take precedence over this prefix.
app.mount(BASE_PATH, mcp_app)
