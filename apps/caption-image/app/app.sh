#!/bin/bash
set -e

SLEEPING_TIME=${SLEEPING_TIME:-30}

python3 status_tools.py --completed 0 --phases processing --phases sleeping --phase processing
while true; do
    python3 status_tools.py --completed 0 --phase processing
    python3 log_processor.py "python3 caption_images.py"

    if [ "$RUN_ONCE" = "true" ]; then
        break
    fi
    python3 status_tools.py --completed 0 --phase sleeping
    sleep $SLEEPING_TIME
done
