#!/bin/bash

set -e # exit on error

# Get the directory this script is in
DIR=$(dirname $(readlink -f $0))
# Extract the name of the directory
NAME=$(basename ${DIR})
# Build an image name from the directory name
IMAGE_NAME="aperturedata/workflows-${NAME}"

# Build the image
cd $DIR
docker build -t ${IMAGE_NAME} .
