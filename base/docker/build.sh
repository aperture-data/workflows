#!/bin/bash
set -e
cd $(dirname "$(readlink -f "$0")")
source ../../.commonrc
build_base