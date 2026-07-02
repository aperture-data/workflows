#!/bin/bash
set -e

SLEEPING_TIME=$(/app/wf_argparse.py --type non_negative_int --envar SLEEPING_TIME --default 30)
RUN_ONCE=$(/app/wf_argparse.py --type bool --envar RUN_ONCE --default false)

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
