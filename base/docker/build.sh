#!/bin/bash
set -e
RUNNER_NAME=${RUNNER_NAME:-$(hostname)}
COMPOSE_PROJECT_NAME="${RUNNER_NAME}-workflows-base"
cd $(dirname "$(readlink -f "$0")")
source ../../.commonrc
$COMMAND build base