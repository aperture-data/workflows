#!/bin/bash

docker build --build-arg WORKFLOW_VERSION=latest -t aperturedata/workflows-base base/docker
for d in .devcontainer/*/; do
  if [ -d "$d" ]; then
    python3 .devcontainer/configuration_params.py > "${d}.env"
  fi
done
