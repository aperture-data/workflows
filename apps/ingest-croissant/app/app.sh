#!/bin/bash
set -e

SAMPLE_COUNT=${WF_SAMPLE_COUNT:--1}

python3 execute_query.py $WF_CROISSANT_URL
adb ingest from-croissant $WF_CROISSANT_URL --sample-count $SAMPLE_COUNT
