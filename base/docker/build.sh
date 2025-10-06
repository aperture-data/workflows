#!/bin/bash
set -e
COMPOSE_PROJECT_NAME="workflows-base"
cd $(dirname "$(readlink -f "$0")")
source ../../.commonrc
$COMMAND build base