#!/bin/bash

# This script builds a Docker image for a specific workflow application.
# Dockerfiles for each workflow application are expected to be in subdirectories of the directory where this script is located.
# The application directory can be specified as an argument, or defaults to the current directory.

set -o pipefail # exit on error in a pipeline
set -u # exit on undefined variable
set -o nounset # exit on undefined variable
set -o errexit # exit on error
set -o errtrace # exit on error in a function
set -o noclobber # prevent overwriting files with redirection


# Get the directory this script is in
BIN_DIR=$(dirname "$(readlink -f "$0")")

# If an argument is provided, use it to find the directory, 
# otherwise use the current directory
if [ $# -gt 0 ]; then
    APP = "$1"
    # App can be the name of a directory or a path to a directory
    if [[ "$APP" == /* ]]; then
        # Absolute path
        DIR=$(realpath "$APP")
    else
        # Relative path
        DIR=$(realpath "${BIN_DIR}/../${APP}")
    fi
else
    # No argument provided, use the current directory
    # This is not the directory where the script is located, but a subdirectory of it
    DIR="$(realpath $(pwd))"
fi

echo "Building Docker image for directory: $DIR"

# Check if the directory exists
if [ ! -d "$DIR" ]; then  
    echo "Error: Directory '$DIR' does not exist."
    exit 1
fi

# Check if the directory contains a Dockerfile
if [ ! -f "${DIR}/Dockerfile" ]; then
    echo "Error: No Dockerfile found in directory '$DIR'."
    echo "Please ensure that the directory contains a Dockerfile."
    exit 1
fi

# Extract the name of the directory
NAME=$(basename "${DIR}")
# Build an image name from the directory name
IMAGE_NAME="aperturedata/workflows-${NAME}"
echo "Image name: ${IMAGE_NAME}"

TOP=$(git rev-parse --show-toplevel)

# Check for uncommitted changes
IS_DIRTY=$(git -C "$TOP" status --porcelain)
if [ -n "$IS_DIRTY" ]; then
  SHA_SUFFIX="+dirty"
  DESCRIPTION_SUFFIX=" (with uncommitted changes)"
  echo "Warning: Uncommitted changes detected in the repository."
else
  SHA_SUFFIX=""
  DESCRIPTION_SUFFIX=""
fi

cd "$DIR"

# CI should provide these variables, but we need defaults for local builds
VERSION="${VERSION:-dev}"
GITHUB_SHA="${GITHUB_SHA:-$(git -C "$TOP" rev-parse HEAD)}"
GITHUB_REPOSITORY="${GITHUB_REPOSITORY:-aperture-data/workflows}"

GITHUB_SHA_FULL="${GITHUB_SHA}${SHA_SUFFIX}"
BUILD_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
SOURCE_PATH_REL=$(realpath --relative-to="$TOP" "$DIR")
SOURCE_URL="https://github.com/${GITHUB_REPOSITORY}/tree/${GITHUB_SHA}"
DOCKERFILE_URL="${SOURCE_URL}/${SOURCE_PATH_REL}/Dockerfile"
DESCRIPTION="Built from ${DOCKERFILE_URL} on ${BUILD_DATE}, version ${VERSION}${DESCRIPTION_SUFFIX}"
echo "Description: ${DESCRIPTION}"


# Build the image
docker build -t "${IMAGE_NAME}" \
    --label "org.opencontainers.image.version=${VERSION}" \
    --label "org.opencontainers.image.revision=${GITHUB_SHA_FULL}" \
    --label "org.opencontainers.image.created=${BUILD_DATE}" \
    --label "org.opencontainers.image.description=${DESCRIPTION}" \
    --label "org.opencontainers.image.source=${SOURCE_URL}" \
    --label "org.opencontainers.image.ref.name=docker.io/${IMAGE_NAME}:${VERSION}" \
    .
