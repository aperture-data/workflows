#!/bin/bash
set -e

LOG_LEVEL=$(/app/wf_argparse.py --type log_level --envar WF_LOG_LEVEL --default WARNING)

echo "[Startup] Launching MCP server with log level: ${LOG_LEVEL}"
fastmcp run app.py:mcp \
    --transport streamable-http \
    --host 0.0.0.0 \
    --port 8000 \
    --log-level ${LOG_LEVEL}
