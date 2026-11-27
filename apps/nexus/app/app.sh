#!/bin/bash
set -e

LOG_LEVEL=$(wf-argparse --type log_level --envar WF_LOG_LEVEL --default WARNING)

echo "[Startup] Launching Aperture Nexus with log level: ${LOG_LEVEL}"
# No access log: the portal's sign-in link carries the admin token in its query string.
uvicorn server:app \
    --host 0.0.0.0 \
    --port 8000 \
    --proxy-headers \
    --forwarded-allow-ips '*' \
    --no-access-log \
    --log-level "${LOG_LEVEL,,}"
