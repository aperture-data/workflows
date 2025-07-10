#!/bin/bash
set -e
GENERATE_EMBEDDINGS=${GENERATE_EMBEDDINGS:-false}

SLEEPING_TIME=${SLEEPING_TIME:-30}

# Only return upon error
python3 status.py --completed 0 --phases processing --phases sleeping --phase processing
while true; do
    python3 status.py --completed 0 --phase processing
    python3 log_processor.py "python3 face_detection.py -generate_embeddings $GENERATE_EMBEDDINGS"

    if [ "$RUN_ONCE" = "true" ]; then
        break
    fi
    python3 status.py --completed 0 --phase sleeping
    sleep $SLEEPING_TIME
done
