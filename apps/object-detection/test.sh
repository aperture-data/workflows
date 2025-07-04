#!/bin/bash
set -e

bash ../build.sh
RUNNER_NAME="$(whoami)"
OD_NW_NAME="${RUNNER_NAME}_object-detection"
OD_DB_NAME="${RUNNER_NAME}_aperturedb"
OD_IMAGE_ADDER_NAME="${RUNNER_NAME}_add_image"


docker stop ${OD_DB_NAME}  || true
docker rm ${OD_DB_NAME} || true
docker network rm ${OD_NW_NAME} || true

docker network create ${OD_NW_NAME}

# Start empty aperturedb instance
docker run -d \
           --name ${OD_DB_NAME} \
           --network ${OD_NW_NAME} \
           -e ADB_MASTER_KEY="admin" \
           -e ADB_KVGD_DB_SIZE="204800" \
           aperturedata/aperturedb-community

sleep 20

# Add images to the db
docker run --name ${OD_IMAGE_ADDER_NAME} \
           --network ${OD_NW_NAME} \
           -e TOTAL_IMAGES=100 \
           -e DB_HOST=${OD_DB_NAME} \
           -v ./input:/app/data \
           --rm \
           aperturedata/wf-add-image

# Run the object detection workflow
docker run \
           --network ${OD_NW_NAME} \
           -e DB_HOST=${OD_DB_NAME} \
           -e RUN_ONCE=true \
           -e MODEL_NAME="frcnn-mobilenet" \
           -e "WF_LOGS_AWS_CREDENTIALS=${WF_LOGS_AWS_CREDENTIALS}" \
           --rm \
           aperturedata/workflows-object-detection

# if CLEANUP is set to true, stop the aperturedb instance and remove the network
if [ "$CLEANUP" = "true" ]; then
    docker stop ${OD_DB_NAME}
    docker network rm ${OD_NW_NAME}
fi
