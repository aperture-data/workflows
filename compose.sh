#!/usr/bin/env bash

# Wrapper for docker-compose commands to initialize some environment variables

set -euo pipefail

# Get the directory this script is in
BIN_DIR=$(dirname "$(readlink -f "$0")")
cd "$BIN_DIR"

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

GITHUB_SHA="${GITHUB_SHA:-$(git -C "$TOP" rev-parse HEAD)}"
GITHUB_REPOSITORY="${GITHUB_REPOSITORY:-aperture-data/workflows}"

# if VERSION is set use it, or build from githash
if [ -n "${VERSION:-}" ]; then
  echo "Using version: $VERSION"
else
  echo "Building version from git hash: $GITHUB_SHA"
  SHORT_SHA=$(echo "$GITHUB_SHA" | cut -c1-12)
  VERSION="local-$SHORT_SHA"
  if [ -n "${IS_DIRTY}" ]; then
    echo "Repository is dirty, appending dirty hash to version."
    DIRTY_HASH=$( { git diff --cached --no-ext-diff ; git diff --no-ext-diff ; } | sha1sum | cut -c1-12)
    VERSION="${VERSION}-${DIRTY_HASH}"
  fi
  echo "Computed version: $VERSION"
fi
export VERSION


export GITHUB_SHA_FULL="${GITHUB_SHA}${SHA_SUFFIX}"
export BUILD_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
SOURCE_PATH_REL=$(realpath --relative-to="$TOP" "$BIN_DIR")
export SOURCE_URL="https://github.com/${GITHUB_REPOSITORY}/tree/${GITHUB_SHA}"
DOCKERFILE_URL="${SOURCE_URL}/${SOURCE_PATH_REL}/Dockerfile"
export DESCRIPTION="Built from ${DOCKERFILE_URL} on ${BUILD_DATE}, version ${VERSION}${DESCRIPTION_SUFFIX}"
echo "Description: ${DESCRIPTION}"

# Forward all args to docker compose
exec docker compose "$@"