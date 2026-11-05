#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$(readlink -f "$0")")"
project="crawl-rag-constraints-$$"
trap 'docker compose -p "$project" -f compose.yml down -v' EXIT
# The package's ca service exits once it has a certificate, so run the tests
# instead of stopping at the first exited container.
docker compose -p "$project" -f compose.yml run --rm tests
