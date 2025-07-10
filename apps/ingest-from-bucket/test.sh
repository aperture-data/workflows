#!/bin/bash
# test.sh - test ingest-from-bucket
set -x
set -euo pipefail

# ensure required environment variables are set

if [ -z "${WF_INGEST_BUCKET_AWS_CREDS}" ]; then
    echo "missing AWS credentials; fail."
    exit 1
fi

if [ -z "${WF_INGEST_BUCKET_GCP_CREDS}" ]; then
    echo "missing GCP credentials; fail."
    exit 1
fi

AWS_ACCESS_KEY_ID=$(jq -r .access_key <<< ${WF_INGEST_BUCKET_AWS_CREDS})
AWS_SECRET_ACCESS_KEY=$(jq -r .secret_key <<< ${WF_INGEST_BUCKET_AWS_CREDS})

bash ../build.sh
export WORKFLOW_NAME="ingest-from-bucket"
RUNNER_NAME="$(whoami)"
PREFIX="${WORKFLOW_NAME}_${RUNNER_NAME}"

NW_NAME="${PREFIX}"
DB_NAME="${PREFIX}_aperturedb"

# both providers use the same bucket name
BUCKET_NAME="wf-ingest-from-bucket-test-data"

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
docker exec ${DB_NAME} apt-get install -y netcat

echo "Waiting for the ${DB_NAME} to be ready..."
until [ "`docker inspect -f {{.State.Health.Status}} ${DB_NAME}`" == "healthy" ]; do
    sleep 1;
done;
echo "${DB_NAME} is ready."

#ingest s3

# test all user-facing portions:
# - ingest images
# - ingest videos
# - ingest pdfs
common=()
common+=( -e "WF_BUCKET=${BUCKET_NAME}")
common+=( -e "WF_INGEST_IMAGES=True")
common+=( -e "WF_INGEST_VIDEOS=True")
common+=( -e "WF_INGEST_PDFs=True")
common+=( -e "DB_HOST=${DB_NAME}" )
common+=( --network ${NW_NAME} )

aws=()
aws+=( -e "WF_CLOUD_PROVIDER=s3" )
aws+=( -e "WF_AWS_ACCESS_KEY_ID=\"$AWS_ACCESS_KEY_ID\"" )
aws+=( -e "WF_AWS_SECRET_ACCESS_KEY=\"$AWS_SECRET_ACCESS_KEY\"" )
docker run --rm  ${common[@]} ${aws[@]} aperturedata/workflows-${WORKFLOW_NAME}

# check data
python3 tests/check_data.py --images 7500 --videos 5 --pdfs 10
# remove data
adb utils execute remove_all --force

gcp=()
aws+=( -e "WF_CLOUD_PROVIDER=gs" )
aws+=( -e "WF_GCP_SERVICE_ACCOUNT_KEY=\"$WF_INGEST_BUCKET_GCP_CREDS\"" )
docker run --rm  ${common[@]} ${aws[@]} aperturedata/workflows-${WORKFLOW_NAME}

# check data
python3 tests/check_data.py --images 7500 --videos 5 --pdfs 10
