#!/bin/bash
set -e

bash build.sh

docker stop $(docker ps -q)  || true
docker rm $(docker ps -a -q) || true
docker network rm object-detection || true

OD_NW_NAME="${RUNNER_NAME}_object-detection"
docker network create ${OD_NW_NAME}

# Start empty aperturedb instance
docker run -d \
           --name aperturedb \
           --network ${OD_NW_NAME} \
           -p 55555:55555 \
           -e ADB_MASTER_KEY="admin" \
           -e ADB_KVGD_DB_SIZE="204800" \
           aperturedata/aperturedb-community

sleep 20

# Add images to the db
docker run --name add_image \
           --network ${OD_NW_NAME} \
           -e TOTAL_IMAGES=100 \
           -e DB_HOST=aperturedb \
           -v ./input:/app/data \
           aperturedata/wf-add-image

# Run the object detection workflow
docker run \
           --network ${OD_NW_NAME} \
           -e DB_HOST=aperturedb \
           -e RUN_ONCE=true \
           -e MODEL_NAME="frcnn-mobilenet" \
           -e "WF_LOGS_AWS_CREDENTIALS=${WF_LOGS_AWS_CREDENTIALS}" \
           aperturedata/workflows-object-detection

# if CLEANUP is set to true, stop the aperturedb instance and remove the network
if [ "$CLEANUP" = "true" ]; then
    docker stop aperturedb
    docker network rm ${OD_NW_NAME}
fi
