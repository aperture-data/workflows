#!/bin/bash

CORPUS="$1"
if [ -z "$CORPUS" ]; then
    echo "Please provide the corpus name"
    exit 1
fi

DATA=gs://${WF_DATA_SOURCE_GCP_BUCKET}/workflows/${CORPUS}
DIR=/app/input/${CORPUS}
gcloud storage rsync --recursive ${DATA} ${DIR}

# Setup coco folder hierarchy
cd $DIR
unzip -u -q $DIR/stuff_${CORPUS}2017_pixelmaps.zip
unzip -u -q $DIR/${CORPUS}2017.zip
tar xf $DIR/${CORPUS}_clip_embeddings.tgz
