#!/bin/bash

set -euo pipefail

/opt/venv/bin/adb config create default --host=${DB_HOST} --port=${DB_PORT} --username=${DB_USER:-admin} --password=${DB_PASS:-admin} --no-interactive
/opt/venv/bin/adb --install-completion