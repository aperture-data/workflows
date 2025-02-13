#!/bin/bash

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

mkdir -p data/original

DATA=s3://${WF_DATA_SOURCE_AWS_BUCKET}/coco/data/original
DIR=/app/input/original
aws s3 sync --quiet $DATA $DIR

# Setup coco folder hierarchy
cd /app/input
unzip -u $DIR/stuff_annotations_trainval2017.zip
unzip -u $DIR/annotations_trainval2017.zip

mkdir -p images
cd images/
unzip -u -q ../annotations/stuff_train2017_pixelmaps.zip
rm ../annotations/stuff_train2017_pixelmaps.zip
unzip -u -q ../annotations/stuff_val2017_pixelmaps.zip
rm ../annotations/stuff_val2017_pixelmaps.zip
unzip -u -q $DIR/test2017.zip
unzip -u -q $DIR/train2017.zip
unzip -u -q $DIR/val2017.zip

echo "Extracting clip embeddings"
cd ..
tar -xf $DIR/val_clip_embeddings.tgz
tar -xf $DIR/train_clip_embeddings.tgz

