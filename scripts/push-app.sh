#!/usr/bin/env bash
set -euo pipefail

image="aperturedata/workflows-${1:?missing app name}:${VERSION:?missing version}"
expected=${INPUT_HASH:?missing input hash}
actual=$(docker image inspect --format '{{ index .Config.Labels "com.aperturedata.input-hash" }}' "$image")
if [[ $actual != "$expected" ]]; then
    echo "$image has input hash '$actual', expected '$expected'" >&2
    exit 1
fi
docker push "$image"
