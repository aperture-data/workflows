#!/bin/bash
set -e

SLEEPING_TIME=${SLEEPING_TIME:-30}

# Only return upon error
while true; do
    wf-status --completed 0 --phases processing --phases sleeping --phase processing
    wf-log-processor 'python3 extract_embeddings.py'

    if [ "${RUN_ONCE,,}" = "true" ]; then
        break
    fi

    wf-status --completed 0 --phases processing --phases sleeping --phase sleeping
    for ((i=1; i<=SLEEPING_TIME; i+=5)); do
        sleep 5
        wf-status --completed $(( i * 100 / SLEEPING_TIME ))
    done
done
