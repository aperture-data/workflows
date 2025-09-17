#!/bin/bash

/opt/venv/bin/adb config create default --host=${DB_HOST} --port=${DB_PORT} --no-interactive
/opt/venv/bin/adb --install-completion