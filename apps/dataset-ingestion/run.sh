#!/bin/bash
# --user $(id -u):$(id -g) \

docker run -it \
    --network host \
    -v /home/ubuntu/Projects/ad/demos/trial/trial_data:/app/input \
    -v /home/ubuntu/Projects/ad/demos/trial/output:/app/output \
    -e "DB_HOST=localhost" \
    -e "BATCH_SIZE=100" \
    -e "NUM_WORKERS=8" \
    -e "SAMPLE_COUNT=1000" \
    -e "DATASET=coco" \
    aperturedata/workflows-dataset-ingestion
