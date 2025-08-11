#!/usr/bin/env bash
set -euo pipefail

WORKFLOW="sql-server"

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

# ---- cleanup on exit ----
cleanup() {
  $COMPOSE_SCRIPT -p "$COMPOSE_PROJECT_NAME" \
    -f "$COMPOSE_MAIN" -f "$COMPOSE_TEST" down -v --remove-orphans || true
}
trap cleanup EXIT

# ---- run tests ----
echo ">>> Running $WORKFLOW tests (project=$COMPOSE_PROJECT_NAME)"

# Build and run. We rely on:
# - aperturedb service from the main compose
# - this workflow’s image from the main compose
# - the test overlay adds seed + tests + healthchecks
$COMPOSE_SCRIPT -v -p "$COMPOSE_PROJECT_NAME" \
  -f "$COMPOSE_MAIN" -f "$COMPOSE_TEST" \
  build test-base
  
$COMPOSE_SCRIPT -v -p "$COMPOSE_PROJECT_NAME" \
  -f "$COMPOSE_MAIN" -f "$COMPOSE_TEST" \
  up aperturedb seed sql-server tests

