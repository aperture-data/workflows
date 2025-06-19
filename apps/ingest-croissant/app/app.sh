#!/bin/bash
set -e


python3 execute_query.py $WF_CROISSANT_URL
adb ingest from-croissant $WF_CROISSANT_URL --sample-count $WF_SAMPLE_COUNT
