#!/bin/bash
set -e

SLEEPING_TIME=${SLEEPING_TIME:-30}

# Only return upon error
while true; do
    python3 eval.py

    if [ "$RUN_ONCE" = "true" ]; then
        break
    fi
    sleep $SLEEPING_TIME=${SLEEPING_TIME:-30}

done
