#!/usr/bin/env bash
set -x
set -euo pipefail
cd $(dirname "$(readlink -f "$0")")
source ../../.commonrc
if [ "$CI_RUN" -eq 0 ]; then
  TEST_PHASE=base_build
  $COMMAND build base
fi
TEST_PHASE=unit_precheck
$COMMAND run --rm --no-deps unit-tests
run_pytest
