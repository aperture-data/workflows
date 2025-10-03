#!/usr/bin/env bash
set -x
set -euo pipefail

WORKFLOW="embeddings-extraction"

# Get the directory this script is in
export BIN_DIR=$(dirname "$(readlink -f "$0")")
export ROOT_DIR=$BIN_DIR/../..
export TEST_DIR=$BIN_DIR/test

echo "BIN_DIR: $BIN_DIR"
echo "ROOT_DIR: $ROOT_DIR"
echo "TEST_DIR: $TEST_DIR"

cd $BIN_DIR

COMPOSE_MAIN="$ROOT_DIR/docker-compose.yml"
COMPOSE_TEST="$TEST_DIR/docker-compose.yml"
COMPOSE_PROJECT_NAME="${WORKFLOW}-tests"
COMPOSE_SCRIPT="$ROOT_DIR/compose.sh"

export DB_HOST DB_PASS
DB_HOST="${DB_HOST:-aperturedb}"
DB_PASS="${DB_PASS:-admin}"
export DB_TCP_CN="lenz"

# ---- cleanup on exit ----
cleanup() {
  $COMPOSE_SCRIPT -p "$COMPOSE_PROJECT_NAME" \
    -f "$COMPOSE_MAIN" -f "$COMPOSE_TEST" down -v --remove-orphans || true
}
trap cleanup EXIT

# ---- run tests ----
echo ">>> Running $WORKFLOW tests (project=$COMPOSE_PROJECT_NAME)"

COMMAND="$COMPOSE_SCRIPT -v -p $COMPOSE_PROJECT_NAME \
  -f $COMPOSE_MAIN -f $COMPOSE_TEST"

# Should the test script be building this workflow?
$COMMAND build base
$COMMAND build test-base embeddings-extraction

# This log file is useful for debugging test failures
TEST_LOG=$BIN_DIR/test.log
echo "Writing logs to $TEST_LOG"
(
  sleep 5
  $COMMAND logs -f > $TEST_LOG
) &
LOG_PID=$!

$COMMAND up --no-build --exit-code-from tests tests
# $COMMAND up --no-build seed

# Wait for logs to finish
kill $LOG_PID || true
