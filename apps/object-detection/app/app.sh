#!/bin/bash
set -e

# Only return upon error
while true; do
    python3 eval.py

    if [ "$RUN_ONCE" = "true" ]; then
        break
    fi
    sleep 30
done
