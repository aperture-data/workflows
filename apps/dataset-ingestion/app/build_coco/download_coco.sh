#!/bin/bash

CORPUS="$1"
if [ -z "$CORPUS" ]; then
    echo "Please provide the corpus name"
    exit 1
fi

if [ -z "${WF_DATA_SOURCE_AWS_BUCKET}" ]; then
    echo "Please set the WF_DATA_SOURCE_AWS_BUCKET environment variable"
    exit 1
fi
if [ -z "${WF_DATA_SOURCE_AWS_CREDENTIALS}" ]; then
    echo "Please set the WF_DATA_SOURCE_AWS_CREDENTIALS environment variable"
    exit 1
fi

AWS_ACCESS_KEY_ID=$(jq -r .access_key <<< ${WF_DATA_SOURCE_AWS_CREDENTIALS})
AWS_SECRET_ACCESS_KEY=$(jq -r .secret_key <<< ${WF_DATA_SOURCE_AWS_CREDENTIALS})


DATA=s3://${WF_DATA_SOURCE_AWS_BUCKET}/coco/data/${CORPUS}
DIR=/app/input/${CORPUS}
aws s3 sync $DATA $DIR

# Setup coco folder hierarchy
cd $DIR
unzip -u $DIR/stuff_${CORPUS}2017_pixelmaps.zip
unzip -u $DIR/${CORPUS}2017.zip
tar xf $DIR/${CORPUS}_clip_embeddings.tgz
