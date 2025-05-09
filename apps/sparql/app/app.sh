#!/bin/bash
set -e

PORT=8001

# Optional: echo startup params
echo "[Startup] Launching API server..."
echo "[Startup] Using Uvicorn to serve app.py on 0.0.0.0:$PORT"

# Start your FastAPI app via Uvicorn
uvicorn app:root_app \
  --host 0.0.0.0 \
  --port $PORT \
  --workers ${UVICORN_WORKERS:-1} \
  --log-level ${UVICORN_LOG_LEVEL:-info}