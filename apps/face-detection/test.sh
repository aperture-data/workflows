#!/bin/bash

set -x
set -euo pipefail
source ../../.commonrc

$COMMAND build base

# This log file is useful for debugging test failures
TEST_LOG=$BIN_DIR/test.log
echo "Writing logs to $TEST_LOG"
(
  sleep 5
  $COMMAND logs -f > $TEST_LOG
) &
LOG_PID=$!

$COMMAND up --exit-code-from ${COMPOSE_PROJECT_NAME} ${COMPOSE_PROJECT_NAME}

# Wait for logs to finish
kill $LOG_PID || true