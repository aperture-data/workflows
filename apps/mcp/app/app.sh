#!/bin/bash
set -e

LOG_LEVEL=${WF_LOG_LEVEL:-INFO}

echo "[Startup] Launching MCP server..."
PYTHONPATH=. fastmcp run app.py:mcp \
    --transport http \
    --host 0.0.0.0 \
    --port 80 \
    --log-level ${LOG_LEVEL} 
