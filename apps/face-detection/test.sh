#!/bin/bash
set -e

bash build.sh
export WORKFLOW_NAME="face-detection"
RUNNER_NAME="$(whoami)"
FD_NW_NAME="${RUNNER_NAME}_${WORKFLOW_NAME}"
FD_DB_NAME="${RUNNER_NAME}_aperturedb"
FD_IMAGE_ADDER_NAME="${RUNNER_NAME}_add_image"

docker stop ${FD_DB_NAME}  || true
docker rm ${FD_DB_NAME} || true
docker network rm ${FD_NW_NAME} || true

docker network create ${FD_NW_NAME}

# Start empty aperturedb instance for coco
docker run -d \
           --name ${FD_DB_NAME} \
           --network ${FD_NW_NAME} \
           -e ADB_MASTER_KEY="admin" \
           -e ADB_KVGD_DB_SIZE="204800" \
           aperturedata/aperturedb-community

sleep 20

# Add images to the db
docker run --name ${FD_IMAGE_ADDER_NAME} \
           --network ${FD_NW_NAME} \
           -e DB_HOST="${FD_DB_NAME}" \
           -e TOTAL_IMAGES=100 \
           --rm \
           aperturedata/wf-add-image

docker run \
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