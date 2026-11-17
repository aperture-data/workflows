#!/usr/bin/env bash
set -x
set -euo pipefail
cd $(dirname "$(readlink -f "$0")")
export SAMPLE_COUNT=${SAMPLE_COUNT:--1}
export MOVIES_FIXTURE_DIR=${MOVIES_FIXTURE_DIR:-"$PWD/test/fixtures/cache"}
bash test/prepare-inputs.sh "$MOVIES_FIXTURE_DIR"
source ../../.commonrc
run_pytest
