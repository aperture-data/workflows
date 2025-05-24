from fastapi import FastAPI
from mcp.server.fastmcp import FastMCP
from starlette.middleware.cors import CORSMiddleware
from decorators import register_tools
import tools
import os


TOKEN = os.getenv('WF_TOKEN')


# -- Create MCP server --
mcp = FastMCP(name="ApertureDB")

# -- Create FastAPI debug app --
debug_app = FastAPI(
    title="ApertureDB Debug",
    version="0.1.0",
    description="Debug endpoints for tool development"
)

register_tools(mcp, debug_app, TOKEN)

# -- Add CORS if needed --
debug_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -- Unified app with route prefixes --
app = FastAPI()
app.mount("/mcp", mcp.app)     # Expose /mcp/messages and /mcp/sse
app.mount("/debug", debug_app)  # Expose /debug/docs and all debug routes
