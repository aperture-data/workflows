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
           aperturedata/aperturedb-community

sleep 10

# Add images to the db
docker run --name add-coco \
           --network ${WORKFLOW_NAME} \
           -e DB_HOST="${WORKFLOW_NAME}-aperturedb" \
           -e INGEST_ONCE=true \
           aperturedata/wf-bench-coco-validation

docker run \
    --name ${WORKFLOW_NAME}-workflow \
    --network ${WORKFLOW_NAME} \
    -e RUN_ONCE=true \
    -e "WF_LOGS_AWS_CREDENTIALS=${WF_LOGS_AWS_CREDENTIALS}" \
    -e DB_HOST="${WORKFLOW_NAME}-aperturedb" \
    -e COLLECT_EMBEDDINGS=true \
    aperturedata/workflows-face-detection

if [ "$CLEANUP" = "true" ]; then
    docker stop ${WORKFLOW_NAME}-aperturedb
    docker network rm ${WORKFLOW_NAME}
fi