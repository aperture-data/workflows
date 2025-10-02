#!/bin/bash
# test.sh - test ingest-from-bucket
set -x
set -euo pipefail

# Unblock the CI.
echo "TODO: Need to run this with correct credentials : https://github.com/aperture-data/workflows/issues/160"
bash ../build.sh
exit $?
### End of Unblock

. test.env
# ensure required environment variables are set

if [ -z "${WF_INGEST_BUCKET_AWS_CREDS}" ]; then
    echo "missing AWS credentials; fail."
    exit 1
fi

if [ -z "${WF_INGEST_BUCKET_GCP_CREDS}" ]; then
    echo "missing GCP credentials; fail."
    exit 1
fi

echo "CREDS [ ${WF_INGEST_BUCKET_AWS_CREDS} ] "
R=$(echo ${WF_INGEST_BUCKET_AWS_CREDS} | jq -r .access_key)
echo $R
AWS_ACCESS_KEY_ID=$(jq -r .access_key <<< ${WF_INGEST_BUCKET_AWS_CREDS})
AWS_SECRET_ACCESS_KEY=$(jq -r .secret_key <<< ${WF_INGEST_BUCKET_AWS_CREDS})

bash ../build.sh

CHECKER_NAME="aperturedata-internal/workflow-ingest-from-bucket-checker"
( cd tests/checker && docker build -t "$CHECKER_NAME" . )

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

# Start empty aperturedb instance for workflow
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

#ingest s3

# test all user-facing portions:
# - ingest images
# - ingest videos
# - ingest pdfs
common=()
common+=( -e "WF_BUCKET=${BUCKET_NAME}")
common+=( -e "WF_INGEST_IMAGES=True")
common+=( -e "WF_INGEST_VIDEOS=True")
common+=( -e "WF_INGEST_PDFS=True")
common+=( -e "DB_HOST=${DB_NAME}" )
common+=( --network ${NW_NAME} )

checker_opts=()
checker_opts+=( -e "IMAGE_COUNT=7500")
checker_opts+=( -e "VIDEO_COUNT=5")
checker_opts+=( -e "PDF_COUNT=10")

aws=()
aws+=( -e "WF_CLOUD_PROVIDER=s3" )
aws+=( -e "WF_AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" )
aws+=( -e "WF_AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" )

set +x
docker run --rm  ${common[@]} ${aws[@]} aperturedata/workflows-${WORKFLOW_NAME}
set -x
# check data
docker run --rm ${common[@]} ${checker_opts[@]} ${CHECKER_NAME}
# remove data
adb utils execute remove_all --force

gcp=()
gcp+=( -e "WF_CLOUD_PROVIDER=gs" )
gcp+=( -e "WF_GCP_SERVICE_ACCOUNT_KEY=\"$WF_INGEST_BUCKET_GCP_CREDS\"" )
set +x
docker run --rm  ${common[@]} ${aws[@]} aperturedata/workflows-${WORKFLOW_NAME}
set -x

# check data
docker run --rm ${common[@]} ${checker_opts[@]} ${CHECKER_NAME}
