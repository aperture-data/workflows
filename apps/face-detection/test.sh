#!/bin/bash
set -e

bash build.sh
export WORKFLOW_NAME="face-detection"
FD_NW_NAME="${RUNNER_NAME}_${WORKFLOW_NAME}"
FD_DB_NAME="${RUNNER_NAME}_aperturedb"

docker stop ${FD_DB_NAME}  || true
docker rm ${FD_DB_NAME} || true
docker network rm ${FD_NW_NAME} || true

docker network create ${FD_NW_NAME}

# Start empty aperturedb instance for coco
docker run -d \
           --name ${FD_DB_NAME} \
           --network ${FD_NW_NAME} \
           -p 55555:55555 \
           -e ADB_MASTER_KEY="admin" \
           -e ADB_KVGD_DB_SIZE="204800" \
           aperturedata/aperturedb-community

sleep 20

# Add images to the db
docker run --name add-image \
           --network ${FD_NW_NAME} \
           -e DB_HOST="${FD_DB_NAME}" \
           -e TOTAL_IMAGES=100 \
           --rm \
           aperturedata/wf-add-image

docker run \
    --name ${WORKFLOW_NAME}-workflow \
    --network ${FD_NW_NAME} \
    -e RUN_ONCE=true \
    -e "WF_LOGS_AWS_CREDENTIALS=${WF_LOGS_AWS_CREDENTIALS}" \
    -e DB_HOST="${FD_DB_NAME}" \
    -e COLLECT_EMBEDDINGS=true \
    --rm \
    aperturedata/workflows-face-detection

if [ "$CLEANUP" = "true" ]; then
    docker stop ${FD_DB_NAME}
    docker network rm ${FD_NW_NAME}
fi