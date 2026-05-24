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
  echo "This can cause lot of tags being created while developing."
  echo "Consider using the VERSION environment variable to set the version."
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

# Workaround for docker compose build failing on older buildx versions
if [[ " $* " == *" build "* ]] || [[ " $* " == *" --build "* ]]; then
    NEEDS_UPDATE=false
    if ! docker buildx version >/dev/null 2>&1; then
        NEEDS_UPDATE=true
    else
        BUILDX_VER=$(docker buildx version | grep -oE 'v?[0-9]+\.[0-9]+\.[0-9]+' | head -n1 | sed 's/v//')
        if [ -n "$BUILDX_VER" ]; then
            if [ "$(printf '%s\n' "0.17.0" "$BUILDX_VER" | sort -V | head -n1)" = "$BUILDX_VER" ] && [ "$BUILDX_VER" != "0.17.0" ]; then
                NEEDS_UPDATE=true
            fi
        fi
    fi

    if [ "$NEEDS_UPDATE" = "true" ]; then
        echo "Workaround: Updating docker-buildx plugin to 0.17.1 to fix compose build issues."
        DOCKER_PLUGIN_DIR="${DOCKER_CONFIG:-$HOME/.docker}/cli-plugins"
        mkdir -p "$DOCKER_PLUGIN_DIR"
        OS=$(uname -s | tr "[:upper:]" "[:lower:]")
        ARCH=$(uname -m)
        case "$ARCH" in
            x86_64|amd64) ARCH="amd64" ;;
            aarch64|arm64) ARCH="arm64" ;;
            *) ARCH="" ;;
        esac
        if [ -n "$ARCH" ]; then
            TMP_DIR=$(mktemp -d)
            curl -fsSL "https://github.com/docker/buildx/releases/download/v0.17.1/checksums.txt" -o "$TMP_DIR/checksums.txt"
            curl -fsSL "https://github.com/docker/buildx/releases/download/v0.17.1/buildx-v0.17.1.${OS}-${ARCH}" -o "$TMP_DIR/buildx"
            if cd "$TMP_DIR" && grep "buildx-v0.17.1.${OS}-${ARCH}$" checksums.txt | sed "s/buildx-v0.17.1.${OS}-${ARCH}/buildx/" | sha256sum --check --status; then
                mv "$TMP_DIR/buildx" "$DOCKER_PLUGIN_DIR/docker-buildx"
                chmod +x "$DOCKER_PLUGIN_DIR/docker-buildx"
            else
                echo "Error: Checksum validation failed for docker-buildx."
                rm -rf "$TMP_DIR"
                exit 1
            fi
            rm -rf "$TMP_DIR"
        else
            echo "Skipping buildx update: unsupported architecture $(uname -m)"
        fi
    fi
fi

# Forward all args to docker compose
exec docker compose "$@"