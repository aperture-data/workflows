#!/bin/bash

set -e

IMG_NAME="aperturedata/workflows-base"

# Build docker image
docker pull ${IMG_NAME}
docker build --cache-from=${IMG_NAME} -t ${IMG_NAME} .
