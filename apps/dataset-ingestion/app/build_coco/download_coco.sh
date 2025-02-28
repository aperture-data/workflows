#!/bin/bash

CORPUS="$1"
if [ -z "$CORPUS" ]; then
    echo "Please provide the corpus name"
    exit 1
fi

DATA=gs://${WF_DATA_SOURCE_GCP_BUCKET}/workflows_streaming/${CORPUS}
DIR=/app/input/${CORPUS}
date
gcloud storage rsync ${DATA} ${DIR}
echo $(date) "Downloaded data from ${DATA} to ${DIR}"
