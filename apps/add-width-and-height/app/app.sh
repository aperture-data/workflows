#!/bin/bash
set -e

SLEEPING_TIME=${SLEEPING_TIME:-30}

# Only return upon error
while true; do
    python3 status_tools.py --completed 0 --phases processing --phases sleeping --phase processing
    python3 log_processor.py 'python3 add_width_and_height_to_images.py'

    if [ "${RUN_ONCE,,}" = "true" ]; then
        break
    fi

    python3 status_tools.py --completed 0 --phases processing --phases sleeping --phase sleeping
    for ((i=1; i<=SLEEPING_TIME; i+=5)); do
        sleep 5
        python3 status_tools.py --completed $(( i * 100 / SLEEPING_TIME ))
    done
done
