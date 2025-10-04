#!/bin/bash

set -x
set -euo pipefail

WORKFLOW="object-detection"

export BIN_DIR=$(dirname "$(readlink -f "$0")")
export ROOT_DIR=$BIN_DIR/../..

cd $BIN_DIR

COMPOSE_MAIN="$ROOT_DIR/docker-compose.yml"
COMPOSE_SCRIPT="$ROOT_DIR/compose.sh"
COMPOSE_PROJECT_NAME="${WORKFLOW}"

export DB_HOST="lenz"
export DB_PORT="55551"
export DB_PASS="admin"
export DB_TCP_CN="lenz"
export DB_HTTP_CN="nginx"

# ---- cleanup on exit ----
cleanup() {
  $COMPOSE_SCRIPT -p "$COMPOSE_PROJECT_NAME" \
    -f "$COMPOSE_MAIN" down -v --remove-orphans || true
}
trap cleanup EXIT

COMMAND="$COMPOSE_SCRIPT -v -p $COMPOSE_PROJECT_NAME \
  -f $COMPOSE_MAIN"

$COMMAND build base

# This log file is useful for debugging test failures
TEST_LOG=$BIN_DIR/test.log
echo "Writing logs to $TEST_LOG"
(
  sleep 5
  $COMMAND logs -f > $TEST_LOG
) &
LOG_PID=$!

$COMMAND up --exit-code-from ${WORKFLOW} ${WORKFLOW}

# Wait for logs to finish
kill $LOG_PID || true