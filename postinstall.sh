#!/bin/bash

set -euo pipefail

/opt/venv/bin/adb config create default --host="${DB_HOST:?DB_HOST must be set}" --port="${DB_PORT:?DB_PORT must be set}" --username="${DB_USER:-admin}" --password="${DB_PASS:-admin}" --no-interactive
/opt/venv/bin/adb --install-completion