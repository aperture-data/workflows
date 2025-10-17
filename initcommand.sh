#!/bin/bash

docker build --build-arg WORKFLOW_VERSION=\"latest\" -t aperturedata/workflows-base base/docker
python3 configuration_params.py > .devcontainer/caption-image/.env