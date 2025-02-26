#!/bin/bash

CORPUS="$1"
if [ -z "$CORPUS" ]; then
    echo "Please provide the corpus name"
    exit 1
fi

DATA=s3://${WF_DATA_SOURCE_AWS_BUCKET}/coco/data/${CORPUS}
DIR=/app/input/${CORPUS}
gcloud storage rsync --recursive gs://ad-demos-datasets/workflows/${CORPUS} $DIR

# Setup coco folder hierarchy
cd $DIR
unzip -u -q $DIR/stuff_${CORPUS}2017_pixelmaps.zip
unzip -u -q $DIR/${CORPUS}2017.zip
tar xf $DIR/${CORPUS}_clip_embeddings.tgz
