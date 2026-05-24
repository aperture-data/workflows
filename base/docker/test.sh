#!/usr/bin/env bash
set -x
set -euo pipefail
cd $(dirname "$(readlink -f "$0")")
source ../../.commonrc

BUILD_WORKFLOW=0 run_pytest