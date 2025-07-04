#!/bin/bash
set -e

bash ../build.sh
RUNNER_NAME="$(whoami)"
EE_NW_NAME="${RUNNER_NAME}_embeddings-extraction"
EE_DB_NAME="${RUNNER_NAME}_aperturedb"
EE_IMAGE_ADDER_NAME="${RUNNER_NAME}_add_image"

docker stop ${EE_DB_NAME}  || true
docker rm ${EE_DB_NAME} || true
docker network rm ${EE_NW_NAME} || true

docker network create ${EE_NW_NAME}

# Start empty aperturedb instance
docker run -d \
           --name ${EE_DB_NAME} \
           --network ${EE_NW_NAME} \
           -e ADB_MASTER_KEY="admin" \
           -e ADB_KVGD_DB_SIZE="204800" \
           aperturedata/aperturedb-community

sleep 20

# Add images to the db
docker run --name ${EE_IMAGE_ADDER_NAME} \
           --network ${EE_NW_NAME} \
           -e TOTAL_IMAGES=100 \
           -e DB_HOST=${EE_DB_NAME} \
           -v ./input:/app/data \
           --rm \
           aperturedata/wf-add-image

# Run the object detection workflow
docker run \
           --network ${EE_NW_NAME} \
           -e DB_HOST=${EE_DB_NAME} \
           -e "WF_LOGS_AWS_CREDENTIALS=${WF_LOGS_AWS_CREDENTIALS}" \
           -e RUN_ONCE=true \
           -e MODEL_NAME="ViT-B/16" \
           --rm \
           aperturedata/workflows-embeddings-extraction

# if CLEANUP is set to true, stop the aperturedb instance and remove the network
if [ "$CLEANUP" = "true" ]; then
    docker stop ${EE_DB_NAME}
    docker network rm ${EE_NW_NAME}
fi
