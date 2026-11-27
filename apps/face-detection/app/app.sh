#!/bin/bash
set -e
GENERATE_EMBEDDINGS=${GENERATE_EMBEDDINGS:-false}

SLEEPING_TIME=${SLEEPING_TIME:-30}

# Only return upon error
wf-status --completed 0 --phases processing --phases sleeping --phase processing
while true; do
    wf-status --completed 0 --phase processing
    wf-log-processor "python3 face_detection.py -generate_embeddings $GENERATE_EMBEDDINGS"

    if [ "$RUN_ONCE" = "true" ]; then
        break
    fi
    wf-status --completed 0 --phase sleeping
    sleep $SLEEPING_TIME
done
