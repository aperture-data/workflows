#!/bin/bash
set -e

SAMPLE_COUNT=${WF_SAMPLE_COUNT:--1}
FLATTEN_JSON=${WF_FLATTEN_JSON:-false}
COMMAND_ARGS="--sample-count $SAMPLE_COUNT --no-flatten-json"

if [ "$FLATTEN_JSON" = "true" ]; then
    COMMAND_ARGS="--sample-count $SAMPLE_COUNT --flatten-json"
fi

echo "Ingesting Croissant dataset from $WF_CROISSANT_URL with sample count $SAMPLE_COUNT and flatten json $FLATTEN_JSON"
echo "Command args: $COMMAND_ARGS"


python3 execute_query.py $WF_CROISSANT_URL
export WF_CROISSANT_URL
export COMMAND_ARGS
python log_processor.py "/app/venv/bin/adb ingest from-croissant $WF_CROISSANT_URL $COMMAND_ARGS"
