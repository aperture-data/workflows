#!/bin/bash
set -e

SLEEPING_TIME=$(wf-argparse --type non_negative_int --envar SLEEPING_TIME --default 30)
RUN_ONCE=$(wf-argparse --type bool --envar RUN_ONCE --default false)

# Only return upon error
while true; do
    wf-status --completed 0 --phases processing --phases sleeping --phase processing
    wf-log-processor 'python3 app.py'

    if [ "${RUN_ONCE}" = "true" ]; then
        break
    fi
    wf-status --completed 0 --phases processing --phases sleeping --phase sleeping
    sleep $SLEEPING_TIME
done
