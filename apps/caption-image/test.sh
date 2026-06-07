#!/bin/bash
set -o pipefail
set -o nounset
set -o errexit

export PRELOAD_MODEL=true
bash ../build.sh
