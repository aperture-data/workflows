#!/bin/bash
set -o pipefail
set -o nounset
set -o errexit

cd "$(dirname "$(readlink -f "$0")")"

bash ../build.sh
