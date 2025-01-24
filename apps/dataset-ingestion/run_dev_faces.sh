#!/bin/bash
# --user $(id -u):$(id -g) \
# -e "DB_HOST=localhost" \

docker run -it \
    --network host \
    -v /home/ubuntu/Projects/ad/demos/celebA/docker/ingest/input:/app/build_faces/input \
    -v /home/ubuntu/Projects/ad/demos/celebA/docker/ingest/output:/app/build_faces/output \
    -e "AWS_ACCESS_KEY_ID=<AWS KEY ID GOES HERE>" \
    -e "AWS_SECRET_ACCESS_KEY=<AWS ACCESS KEY >" \
    -e "DB_HOST=localhost" \
    -e "BATCH_SIZE=100" \
    -e "NUM_WORKERS=8" \
    -e "CLEAN=true" \
    -e "SAMPLE_COUNT=1000" \
    -e "LOAD_CELEBAHQ=true" \
    -e "DATASET=faces" \
    aperturedata/workflows-dataset-ingestion
