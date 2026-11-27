#!/bin/bash
set -e

SLEEPING_TIME=$(wf-argparse --type non_negative_int --envar SLEEPING_TIME --default 30)
RUN_ONCE=$(wf-argparse --type bool --envar RUN_ONCE --default false)

wf-status --completed 0 --phases processing --phases sleeping --phase processing
while true; do
    wf-status --completed 0 --phase processing
    wf-log-processor "python3 caption_images.py"

    if [ "$RUN_ONCE" = "true" ]; then
        break
    fi
    wf-status --completed 0 --phase sleeping
    sleep $SLEEPING_TIME
done
