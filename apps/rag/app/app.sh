#!/bin/bash
set -e

PORT=8000

# Optional: echo startup params
echo "[Startup] Launching RAG API server..."
echo "[Startup] Using Uvicorn to serve app.py on 0.0.0.0:$PORT"

# Start your FastAPI app via Uvicorn
uvicorn app:app \
  --host 0.0.0.0 \
  --port $PORT \
  --workers ${UVICORN_WORKERS:-1} \
  --log-level ${UVICORN_LOG_LEVEL:-info}