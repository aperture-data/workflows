#!/bin/bash
set -e

# Get the directory this script is in
DIR=$(dirname $(readlink -f $0))

IMG_NAME="aperturedata/workflows-base"

# Build docker image
cd $DIR
docker build --build-arg WORKFLOW_VERSION=${VERSION} -t ${IMG_NAME}:${VERSION} .
