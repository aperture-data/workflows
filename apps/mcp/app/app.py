from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi import Depends, HTTPException, status
import os
import asyncio

from fastmcp import FastMCP
# from fastmcp.server.auth import BearerAuthProvider

from decorators import register_tools
import tools
from shared import logger
from shared import args

TOKEN = args.auth_token

# LOG_LEVEL = os.getenv('WF_LOG_LEVEL', 'INFO').upper()
# logger.info(f"Starting MCP server with log level: {LOG_LEVEL}")
# print(f"Starting MCP server with log level: {LOG_LEVEL}")

security = HTTPBearer(auto_error=True)


def test_connection():
    """Test connection to the ApertureDB database."""
    from aperturedb.CommonLibrary import create_connector
    client = create_connector()
    from aperturedb.Utils import Utils
    utils = Utils(client)
    utils.summary()


def require_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Simple token authentication dependency."""
    assert TOKEN, "WF_TOKEN environment variable must be set"
    if credentials.credentials != TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid bearer token",
        )


test_connection()

mcp = FastMCP(
    name="ApertureDB",
    instructions="""
    This  MCP server providers access to an instance of the ApertureDB database.
    This is a multi-modal hybrid graph and vector database.
    It supports both vector and graph queries, allowing you to perform complex data retrieval and analysis.
    """,
    # tags=["ApertureDB", "graph", "vector", "database", "similarity",
    #       "text", "image", "video", "audio", "multi-modal", "retrieval", "search", "query"],
    # log_level=LOG_LEVEL,
    dependencies=[Depends(require_token)],
    # path="/mcp",
)

register_tools(mcp)

#     await mcp.run_async(
#         transport="http",  # no streaming required
#         host="0.0.0.0",
#         port=80,
#         path="/mcp",
#         # cors_origins=["*"],  # allow all origins
#     )

# if __name__ == "__main__":
#     asyncio.run(main())
