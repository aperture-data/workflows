#!/bin/bash
# test.sh - test ingest-from-bucket
set -x
set -euo pipefail

# Unblock the CI.
echo "TODO: Need to run this with correct credentials : https://github.com/aperture-data/workflows/issues/160"
bash ../build.sh
exit $?
