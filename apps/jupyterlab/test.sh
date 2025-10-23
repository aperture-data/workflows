#!/bin/bash
set -x
set -euo pipefail
cd $(dirname "$(readlink -f "$0")")
source ../../.commonrc

if [ $CI_RUN -eq 0 ]; then
  $COMMAND build base
fi

$COMMAND build jupyterlab

$COMMAND up -d --wait ${COMPOSE_PROJECT_NAME}
ret=$?

exit $ret