#!/bin/bash
set -e

LOG_LEVEL=${WF_LOG_LEVEL:-INFO}

echo "[Startup] Launching MCP server with log level: ${LOG_LEVEL}"
PYTHONPATH=. fastmcp run app.py:mcp \
    --transport streamable-http \
    --host 0.0.0.0 \
    --port 8000 \
    --log-level ${LOG_LEVEL} 
