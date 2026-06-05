#!/bin/bash

set -euo pipefail

# Make the script self-contained by ensuring it runs from its own directory
cd "$(dirname "${BASH_SOURCE[0]}")"

if ! command -v python3 &> /dev/null; then
    echo "Error: python3 is required on the host to run initcommand.sh." >&2
    exit 1
fi

docker build --build-arg WORKFLOW_VERSION=latest -t aperturedata/workflows-base base/docker
for d in .devcontainer/*/; do
  if [ -d "$d" ]; then
    python3 .devcontainer/configuration_params.py > "${d}.env"
  fi
done
