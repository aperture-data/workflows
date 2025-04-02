#!/bin/bash
set -e

bash build.sh
export WORKFLOW_NAME="face-detection"
docker stop $(docker ps -q)  || true
docker rm $(docker ps -a -q) || true
docker network rm ${WORKFLOW_NAME} || true

FD_NW_NAME="${RUNNER_NAME}_${WORKFLOW_NAME}"
docker network create ${FD_NW_NAME}

# Start empty aperturedb instance for coco
docker run -d \
           --name ${WORKFLOW_NAME}-aperturedb \
           --network ${FD_NW_NAME} \
           -p 55555:55555 \
           -e ADB_MASTER_KEY="admin" \
           -e ADB_KVGD_DB_SIZE="204800" \
           aperturedata/aperturedb-community

sleep 20

# Add images to the db
docker run --name add-image \
           --network ${FD_NW_NAME} \
           -e DB_HOST="${WORKFLOW_NAME}-aperturedb" \
           -e TOTAL_IMAGES=100 \
           aperturedata/wf-add-image

docker run \
    --name ${WORKFLOW_NAME}-workflow \
    --network ${FD_NW_NAME} \
    -e RUN_ONCE=true \
    -e "WF_LOGS_AWS_CREDENTIALS=${WF_LOGS_AWS_CREDENTIALS}" \
    -e DB_HOST="${WORKFLOW_NAME}-aperturedb" \
    -e COLLECT_EMBEDDINGS=true \
    aperturedata/workflows-face-detection

if [ "$CLEANUP" = "true" ]; then
    docker stop ${WORKFLOW_NAME}-aperturedb
    docker network rm ${FD_NW_NAME}
fi