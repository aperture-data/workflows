#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$(readlink -f "$0")")"
project="crawl-rag-constraints-$$"
trap 'docker compose -p "$project" -f compose.yml down -v' EXIT
docker compose -p "$project" -f compose.yml up --abort-on-container-exit --exit-code-from tests
