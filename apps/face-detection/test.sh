#!/bin/bash
set -e

bash build.sh
export WORKFLOW_NAME="face-detection"
docker stop $(docker ps -q)  || true
docker rm $(docker ps -a -q) || true
docker network rm ${WORKFLOW_NAME} || true

docker network create ${WORKFLOW_NAME}

# Start empty aperturedb instance for coco
docker run -d \
           --name ${WORKFLOW_NAME}-aperturedb \
           --network ${WORKFLOW_NAME} \
           -p 55555:55555 \
           -e ADB_MASTER_KEY="admin" \
           -e ADB_KVGD_DB_SIZE="204800" \
           aperturedata/aperturedb:vci_develop

sleep 5

# Add images to the db
docker run --name add_images \
           --network ${WORKFLOW_NAME} \
           -e SAMPLE_COUNT=100 \
           -e DB_HOST="${WORKFLOW_NAME}-aperturedb" \
           aperturedata/coco_val_images

docker run -it \
    --name ${WORKFLOW_NAME}-workflow \
    --network ${WORKFLOW_NAME} \
    -e RUN_ONCE=true \
    -e DB_HOST="${WORKFLOW_NAME}-aperturedb" \
    aperturedata/workflows-face-detection

if [ "$CLEANUP" = "true" ]; then
    docker stop ${WORKFLOW_NAME}-aperturedb
    docker network rm ${WORKFLOW_NAME}
fi