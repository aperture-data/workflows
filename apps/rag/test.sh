#!/bin/bash
set -x
set -euo pipefail

WORKFLOW="rag"

# Get the directory this script is in
export BIN_DIR=$(dirname "$(readlink -f "$0")")
export ROOT_DIR=$BIN_DIR/../..
export TEST_DIR=$BIN_DIR/test

echo "BIN_DIR: $BIN_DIR"
echo "ROOT_DIR: $ROOT_DIR"

cd $BIN_DIR

COMPOSE_MAIN="$ROOT_DIR/docker-compose.yml"
COMPOSE_PROJECT_NAME="${WORKFLOW}-tests"
COMPOSE_SCRIPT="$ROOT_DIR/compose.sh"

export DB_HOST DB_PASS
DB_HOST="${DB_HOST:-lenz}"
DB_PASS="${DB_PASS:-admin}"
export DB_PORT=55551
export DB_TCP_CN="lenz"

# ---- cleanup on exit ----
cleanup() {
  $COMPOSE_SCRIPT -p "$COMPOSE_PROJECT_NAME" \
    -f "$COMPOSE_MAIN" down -v --remove-orphans || true
}
trap cleanup EXIT

# ---- run tests ----
echo ">>> Running $WORKFLOW tests (project=$COMPOSE_PROJECT_NAME)"

COMMAND="$COMPOSE_SCRIPT -v -p $COMPOSE_PROJECT_NAME \
  -f $COMPOSE_MAIN"

if [ ${CI_RUN:-0} -eq 0 ]; then
  $COMMAND build base
fi

$COMMAND build crawl-website text-extraction text-embeddings

# This log file is useful for debugging test failures
TEST_LOG=$BIN_DIR/test.log
echo "Writing logs to $TEST_LOG"
(
  sleep 5
  $COMMAND logs -f > $TEST_LOG
) &
LOG_PID=$!

$COMMAND up -d $WORKFLOW $WORKFLOW
ret=$?
sleep 20

docker logs rag-tests-crawl-website-1
docker logs rag-tests-text-extraction-1
docker logs rag-tests-text-embeddings-1
docker logs rag-tests-$WORKFLOW-1

exit $ret