#!/bin/bash
set -e

# export FASTMCP_PORT=8002 
# Optional: echo startup params
echo "[Startup] Launching MCP server..."
# exec mcp run app.py
exec python3 app.py