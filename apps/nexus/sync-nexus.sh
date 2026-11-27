#!/usr/bin/env bash
# Copy the aperture-nexus package into nexus/ from a committed revision.
#
# Usage: ./sync-nexus.sh [path to an aperture-nexus checkout] [revision]
# Defaults: the sibling checkout next to this repository, at HEAD.

set -euo pipefail

cd "$(dirname "$(readlink -f "$0")")"
SOURCE=$(realpath "${1:-../../../aperture-nexus}")
REVISION=$(git -C "$SOURCE" rev-parse --verify "${2:-HEAD}^{commit}")
PATHS=(pyproject.toml README.md LICENSE src)

rm -rf nexus
mkdir nexus
git -C "$SOURCE" archive "$REVISION" "${PATHS[@]}" | tar -x -C nexus
echo "$REVISION" > nexus/SOURCE_COMMIT
echo "Copied aperture-nexus $REVISION into $(pwd)/nexus"
