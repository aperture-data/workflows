#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$(readlink -f "$0")")"
project="crawl-rag-constraints-$$"
certdir=$(mktemp -d)
cleanup() {
    docker compose -p "$project" -f compose.yml down -v
    rm -rf "$certdir"
}
trap cleanup EXIT
# Native Lenz clients verify the generated stack certificate.
docker compose -p "$project" -f compose.yml up -d aperturedb lenz
docker compose -p "$project" -f compose.yml cp lenz:/etc/lenz/certificate/tls.crt "$certdir/ca.crt"
docker compose -p "$project" -f compose.yml run --rm \
    -v "$certdir/ca.crt:/ca.crt:ro" -e CA_CERT=/ca.crt tests
