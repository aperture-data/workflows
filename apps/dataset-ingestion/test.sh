#!/bin/bash
set -e

bash build.sh
export WORKFLOW_NAME="dataset-ingestion"
docker stop $(docker ps -q)  || true
docker rm $(docker ps -a -q) || true
docker network rm ${WORKFLOW_NAME} || true

docker network create ${WORKFLOW_NAME}

# Start empty aperturedb instance
docker run -d \
           --name aperturedb \
           --network ${WORKFLOW_NAME} \
           -p 55555:55555 \
           -e ADB_MASTER_KEY="admin" \
           aperturedata/aperturedb:vci_develop

sleep 20

#Ingest and verify COCO
docker run -it \
    --network ${WORKFLOW_NAME} \
    -e "DB_HOST=aperturedb" \
    -e "BATCH_SIZE=100" \
    -e "NUM_WORKERS=8" \
    -e "SAMPLE_COUNT=1000" \
    -e "DATASET=coco" \
    aperturedata/workflows-dataset-ingestion

#Ingest and verify Faces
docker run -it \
    --network ${WORKFLOW_NAME} \
    -e "DB_HOST=aperturedb" \
    -e "BATCH_SIZE=100" \
    -e "NUM_WORKERS=8" \
    -e "CLEAN=true" \
    -e "SAMPLE_COUNT=1000" \
    -e "LOAD_CELEBAHQ=true" \
    -e "DATASET=faces" \
    aperturedata/workflows-dataset-ingestion


# if CLEANUP is set to true, stop the aperturedb instance and remove the network
if [ "$CLEANUP" = "true" ]; then
    docker stop aperturedb
    docker network rm ${WORKFLOW_NAME}
fi