#!/bin/bash

set -e # exit on error

# Get the directory this script is in
DIR=$(dirname "$(readlink -f "$0")")
# Extract the name of the directory
NAME=$(basename "${DIR}")
# Build an image name from the directory name
IMAGE_NAME="aperturedata/workflows-${NAME}"


TOP=$(git rev-parse --show-toplevel)

# Check for uncommitted changes
IS_DIRTY=$(git -C "$TOP" status --porcelain)
if [ -n "$IS_DIRTY" ]; then
  SHA_SUFFIX="+dirty"
  DESCRIPTION_SUFFIX=" (with uncommitted changes)"
else
  SHA_SUFFIX=""
  DESCRIPTION_SUFFIX=""
fi

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
cd "$DIR"
docker build -t "${IMAGE_NAME}" \
    --label "org.opencontainers.image.version=${VERSION}" \
    --label "org.opencontainers.image.revision=${GITHUB_SHA_FULL}" \
    --label "org.opencontainers.image.created=${BUILD_DATE}" \
    --label "org.opencontainers.image.description=${DESCRIPTION}" \
    --label "org.opencontainers.image.source=${SOURCE_URL}" \
    --label "org.opencontainers.image.ref.name=docker.io/${IMAGE_NAME}:${VERSION}" \
    .
