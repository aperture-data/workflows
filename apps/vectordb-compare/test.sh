#!/bin/bash
set -e

source $(dirname "$0")/../../tests/utils.sh
cleanup_docker

run_local_aperturedb

docker run --name $WORKFLOWNAME \
           --network $DOCKERNETWORK \
           -e RUN_NAME=ci_test \
           -e DB_HOST=aperturedb \
           -e USE_SSL=false \
           -e MINIMAL=true \
           -e PUSH_TO_S3=true \
           -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
           -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
           -v ./input:/app/input/ \
           aperturedata/wf-$WORKFLOWNAME
