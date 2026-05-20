#!/bin/bash

set -x
set -euo pipefail
cd $(dirname "$(readlink -f "$0")")
source ../../.commonrc

if [ $CI_RUN -eq 0 ]; then
  $COMMAND build base
fi

# Pre-build the image using standard docker build to avoid docker compose buildx 0.17 requirement on some runners
if [ -n "${VERSION:-}" ]; then
  echo "Pre-building aperturedata/workflows-dataset-ingestion:${VERSION}"
  docker build -t aperturedata/workflows-dataset-ingestion:${VERSION} \
    --build-arg VERSION=${VERSION} \
    --build-arg GITHUB_SHA_FULL=${GITHUB_SHA_FULL:-} \
    --build-arg BUILD_DATE=${BUILD_DATE:-} \
    --build-arg DESCRIPTION="${DESCRIPTION:-}" \
    --build-arg SOURCE_URL=${SOURCE_URL:-} \
    --build-arg WORKFLOW_VERSION=${VERSION} \
    -f Dockerfile .
fi

# This log file is useful for debugging test failures
TEST_LOG=$BIN_DIR/test.log
echo "Writing logs to $TEST_LOG"
(
  sleep 5
  $COMMAND logs -f > $TEST_LOG
) &
LOG_PID=$!

$COMMAND up --exit-code-from ${COMPOSE_PROJECT_NAME} ${COMPOSE_PROJECT_NAME}
cleanup
export DATASET="faces"
$COMMAND up --exit-code-from ${COMPOSE_PROJECT_NAME} ${COMPOSE_PROJECT_NAME}
