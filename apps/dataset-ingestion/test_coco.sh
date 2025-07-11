set -e

WF_DATA_SOURCE_GCP_BUCKET="ad-demos-datasets"

bash build.sh
export WORKFLOW_NAME="dataset-ingestion"
docker stop $(docker ps -q)  || true
docker rm $(docker ps -a -q) || true
docker network rm ${WORKFLOW_NAME}_coco || true

docker network create ${WORKFLOW_NAME}_coco
# Start empty aperturedb instance for coco
docker run -d \
           --name aperturedb_coco \
           --network ${WORKFLOW_NAME}_coco \
           -p 55556:55555 \
           -e ADB_MASTER_KEY="admin" \
           -e ADB_KVGD_DB_SIZE="204800" \
           aperturedata/aperturedb-community

sleep 10

docker run \
    --network ${WORKFLOW_NAME}_coco \
    -e "WF_LOGS_AWS_CREDENTIALS=${WF_LOGS_AWS_CREDENTIALS}" \
    -e WF_DATA_SOURCE_GCP_BUCKET=${WF_DATA_SOURCE_GCP_BUCKET} \
    -e "DB_HOST=aperturedb_coco" \
    -e "BATCH_SIZE=100" \
    -e "NUM_WORKERS=8" \
    -e "SAMPLE_COUNT=-1" \
    -e "DATASET=coco" \
    aperturedata/workflows-dataset-ingestion