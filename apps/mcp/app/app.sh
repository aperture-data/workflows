#!/bin/bash
set -e

echo "[Startup] Launching MCP server..."
# exec uvicorn app:app --port 8002 --log-level debug
exec python3 app.py