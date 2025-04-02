#!/bin/bash
set -e

bash build.sh
export WORKFLOW_NAME="dataset-ingestion"
RUNNER_NAME="$(whoami)"
PREFIX="${WORKFLOW_NAME}_${RUNNER_NAME}"

COCO_NW_NAME="${PREFIX}_coco"
CELEBA_NW_NAME="${PREFIX}_celeba"
COCO_DB_NAME="${PREFIX}_aperturedb_coco"
CELEBA_DB_NAME="${PREFIX}_aperturedb_celeba"


docker stop ${COCO_DB_NAME} ${CELEBA_DB_NAME}  || true
docker rm ${COCO_DB_NAME} ${CELEBA_DB_NAME} || true
docker network rm ${COCO_NW_NAME} || true
docker network rm ${CELEBA_NW_NAME} || true

docker network create ${COCO_NW_NAME}
docker network create ${CELEBA_NW_NAME}

# Start empty aperturedb instance for coco
docker run -d \
           --name ${COCO_DB_NAME} \
           --network ${COCO_NW_NAME} \
           -p 55555:55555 \
           -e ADB_MASTER_KEY="admin" \
           -e ADB_KVGD_DB_SIZE="204800" \
           aperturedata/aperturedb-community

docker run -d \
           --name ${CELEBA_DB_NAME} \
           --network ${CELEBA_NW_NAME} \
           -p 55556:55555 \
           -e ADB_MASTER_KEY="admin" \
           -e ADB_KVGD_DB_SIZE="204800" \
           aperturedata/aperturedb-community

sleep 20

#Ingest and verify COCO
docker run \
    --rm \
    --network ${COCO_NW_NAME} \
    -e "WF_LOGS_AWS_CREDENTIALS=${WF_LOGS_AWS_CREDENTIALS}" \
    -e WF_DATA_SOURCE_GCP_BUCKET=${WF_DATA_SOURCE_GCP_BUCKET} \
    -e "DB_HOST=${COCO_DB_NAME}" \
    -e "BATCH_SIZE=100" \
    -e "NUM_WORKERS=8" \
    -e "SAMPLE_COUNT=-1" \
    -e "DATASET=coco" \
    aperturedata/workflows-dataset-ingestion &
pid1=$!


#Ingest and verify Faces
docker run \
    --rm \
    --network ${CELEBA_NW_NAME} \
    -e "WF_LOGS_AWS_CREDENTIALS=${WF_LOGS_AWS_CREDENTIALS}" \
    -e WF_DATA_SOURCE_GCP_BUCKET=${WF_DATA_SOURCE_GCP_BUCKET} \
    -e "DB_HOST=${CELEBA_DB_NAME}" \
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
    docker stop ${COCO_DB_NAME}
    docker stop ${CELEBA_DB_NAME}
    docker network rm ${COCO_NW_NAME}
    docker network rm ${CELEBA_NW_NAME}
fi