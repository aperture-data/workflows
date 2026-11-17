#!/usr/bin/env bash
set -x
set -euo pipefail
cd $(dirname "$(readlink -f "$0")")
source ../../.commonrc
if [ "$CI_RUN" -eq 0 ]; then
  $COMMAND build base
fi
$COMMAND run --rm --no-deps unit-tests
run_pytest
