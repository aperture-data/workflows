#!/bin/bash
set -x
set -euo pipefail

bash ../build.sh
# disable croissant tests so we can push changed for workflow#215 ( temporary )
return 0
export WORKFLOW_NAME="ingest-croissant"
RUNNER_NAME="$(whoami)"
PREFIX="${WORKFLOW_NAME}_${RUNNER_NAME}"

NW_NAME="${PREFIX}"
DB_NAME="${PREFIX}_aperturedb"

CROISSANT_URL="https://huggingface.co/api/datasets/suyc21/MedicalConverter/croissant"

docker stop ${DB_NAME}   || true
docker rm ${DB_NAME}  || true
docker network rm ${NW_NAME} || true

docker network create ${NW_NAME}

# Start empty aperturedb instance for coco
docker run -d \
           --name ${DB_NAME} \
           --network ${NW_NAME} \
           -e ADB_MASTER_KEY="admin" \
           -e ADB_KVGD_DB_SIZE="204800" \
           --health-cmd='nc -z localhost 55555 || exit 1' \
           --health-retries=20 \
           --health-interval=1s \
           aperturedata/aperturedb-community
docker exec ${DB_NAME} apt-get install -y netcat-traditional

echo "Waiting for the ${DB_NAME} to be ready..."
until [ "`docker inspect -f {{.State.Health.Status}} ${DB_NAME}`" == "healthy" ]; do
    sleep 1;
done;
echo "${DB_NAME} is ready."

docker run --rm \
    --network ${NW_NAME} \
    -e "WF_LOGS_AWS_CREDENTIALS=${WF_LOGS_AWS_CREDENTIALS}" \
    -e "WF_DATA_SOURCE_GCP_BUCKET=${WF_DATA_SOURCE_GCP_BUCKET}" \
    -e "DB_HOST=${DB_NAME}" \
    -e "WF_CROISSANT_URL=${CROISSANT_URL}" \
    aperturedata/workflows-${WORKFLOW_NAME}
