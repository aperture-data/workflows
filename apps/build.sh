#!/bin/bash

# This script builds a Docker image for a specific workflow application.
# Dockerfiles for each workflow application are expected to be in subdirectories of the directory where this script is located.
# The application directory can be specified as an argument, or defaults to the current directory.

set -o pipefail # exit on error in a pipeline
set -o nounset # exit on undefined variable
set -o errexit # exit on error
set -o errtrace # exit on error in a function
set -o noclobber # prevent overwriting files with redirection


# Get the directory this script is in
BIN_DIR=$(dirname "$(readlink -f "$0")")
WORKFLOW_VERSION=${WORKFLOW_VERSION:-unset}

echo "Building WORKFLOW_VERSION=$WORKFLOW_VERSION"
# If an argument is provided, use it to find the directory,
# otherwise use the current directory
if [ $# -gt 0 ]; then
    APP="$1"
    # App can be the name of a directory or a path to a directory
    if [[ "$APP" == /* ]]; then
        # Absolute path
        DIR=$(realpath "$APP")
    else
        # Relative path
        DIR=$(realpath "${BIN_DIR}/../apps/${APP}")
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



cd "$DIR"


source ../../.commonrc
if [ $CI_RUN -eq 0 ]; then
  $COMMAND build base
fi

$COMMAND build ${COMPOSE_PROJECT_NAME} ${COMPOSE_PROJECT_NAME}
