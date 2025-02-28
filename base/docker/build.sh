#!/bin/bash
set -e

IMG_NAME="aperturedata/workflows-base"

# Build docker image
docker build --no-cache -t ${IMG_NAME} .


