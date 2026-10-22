#!/usr/bin/env bash
set -x
set -euo pipefail
cd $(dirname "$(readlink -f "$0")")
source ../../.commonrc
$COMMAND run --rm --no-deps unit-tests
run_pytest
