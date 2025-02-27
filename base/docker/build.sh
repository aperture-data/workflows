#!/bin/bash

set -e
PUSH=${1:-"no"}

IMG_NAME="aperturedata/workflows-base"

# Build docker image
if [ $PUSH == "push" ]; then
    docker build --no-cache -t ${IMG_NAME} .
else
    docker pull ${IMG_NAME}
    docker build --cache-from=${IMG_NAME} -t ${IMG_NAME} .
fi

