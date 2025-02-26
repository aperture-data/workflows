#!/bin/bash
set -e

bash build.sh
export WORKFLOW_NAME="dataset-ingestion"
docker stop $(docker ps -q)  || true
docker rm $(docker ps -a -q) || true
docker network rm ${WORKFLOW_NAME}_coco || true
docker network rm ${WORKFLOW_NAME}_celeba || true

docker network create ${WORKFLOW_NAME}_coco
docker network create ${WORKFLOW_NAME}_celeba

# Start empty aperturedb instance for coco
docker run -d \
           --name aperturedb_coco \
           --network ${WORKFLOW_NAME}_coco \
           -p 55555:55555 \
           -e ADB_MASTER_KEY="admin" \
           -e ADB_KVGD_DB_SIZE="204800" \
           aperturedata/aperturedb-community

docker run -d \
           --name aperturedb_celeba \
           --network ${WORKFLOW_NAME}_celeba \
           -p 55556:55555 \
           -e ADB_MASTER_KEY="admin" \
           -e ADB_KVGD_DB_SIZE="204800" \
           aperturedata/aperturedb-community


sleep 20

#Ingest and verify COCO
docker run \
    --network ${WORKFLOW_NAME}_coco \
    -e "WF_LOGS_AWS_CREDENTIALS=${WF_LOGS_AWS_CREDENTIALS}" \
    -e "DB_HOST=aperturedb_coco" \
    -e "BATCH_SIZE=100" \
    -e "NUM_WORKERS=8" \
    -e "SAMPLE_COUNT=-1" \
    -e "DATASET=coco" \
    aperturedata/workflows-dataset-ingestion &
pid1=$!


#Ingest and verify Faces
docker run \
    --network ${WORKFLOW_NAME}_celeba \
    -e "WF_LOGS_AWS_CREDENTIALS=${WF_LOGS_AWS_CREDENTIALS}" \
    -e "DB_HOST=aperturedb_celeba" \
    -e "BATCH_SIZE=100" \
    -e "NUM_WORKERS=8" \
    -e "CLEAN=true" \
    -e "SAMPLE_COUNT=-1" \
    -e "LOAD_CELEBAHQ=true" \
    -e "DATASET=faces" \
    aperturedata/workflows-dataset-ingestion &
pid2=$!

wait $pid1
exit_code1=$?

wait $pid2
exit_code2=$?

if [ $exit_code1 -eq 0 ] && [ $exit_code2 -eq 0 ]; then
  echo "Both ingestion succeeded."
  exit 0
else
  echo "At least one ingestion failed. $exit_code1 $exit_code2"
  exit 1
fi

# if CLEANUP is set to true, stop the aperturedb instance and remove the network
if [ "$CLEANUP" = "true" ]; then
    docker stop aperturedb_coco
    docker stop aperturedb_celeba
    docker network rm ${WORKFLOW_NAME}_coco
    docker network rm ${WORKFLOW_NAME}_celeba
fi