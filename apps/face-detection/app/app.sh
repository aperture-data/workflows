#!/bin/bash
set -e
GENERATE_EMBEDDINGS=${GENERATE_EMBEDDINGS:-false}

SLEEPING_TIME=${SLEEPING_TIME:-30}

# Only return upon error
while true; do
    python3 face_detection.py -generate_embeddings $GENERATE_EMBEDDINGS

    if [ "$RUN_ONCE" = "true" ]; then
        break
    fi
    sleep $SLEEPING_TIME
done
