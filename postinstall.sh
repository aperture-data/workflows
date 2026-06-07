#!/bin/bash

set -euo pipefail

PARAMS=()

if [ "${USE_SSL:-true}" == "false" ]; then
    PARAMS+=(--no-use-ssl)
elif [ "${VERIFY_HOSTNAME:-true}" == "false" ]; then
    PARAMS+=(--no-verify-hostname)
fi

if [ "${USE_REST:-false}" == "true" ]; then
    PARAMS+=(--use-rest)
fi

if [[ -n "${CA_CERT:-}" ]]; then
    PARAMS+=(--ca-cert "$CA_CERT")
fi

/opt/venv/bin/adb config create default \
    --host="${DB_HOST:?DB_HOST must be set}" \
    --port="${DB_PORT:?DB_PORT must be set}" \
    --username="${DB_USER:-admin}" \
    --password="${DB_PASS:-admin}" \
    "${PARAMS[@]}" \
    --no-interactive

echo "/opt/venv/bin/adb --install-completion" | bash || true
