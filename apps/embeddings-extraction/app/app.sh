#!/bin/bash
set -e

SLEEPING_TIME=${SLEEPING_TIME:-30}

# Only return upon error
while true; do
    python3 status.py --completed 0 --phases processing --phases sleeping --phase processing
    python3 log_processor.py 'python3 extract_embeddings.py'

    if [ "${RUN_ONCE,,}" = "true" ]; then
        break
    fi
    python3 status.py --completed 0 --phases processing --phases sleeping --phase sleeping
    sleep $SLEEPING_TIME
done
