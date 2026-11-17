#!/usr/bin/env bash
# Keep the original full dataset outside source control and the product image.
set -euo pipefail

fixture_dir=$1
mkdir -p "$fixture_dir"

fetch_checked() {
  local url=$1 destination=$2 checksum=$3 temporary
  if [ -f "$destination" ] &&
      printf '%s  %s\n' "$checksum" "$destination" | sha256sum --check --status; then
    return
  fi
  temporary=$(mktemp "${destination}.XXXXXX")
  if curl --fail --silent --show-error --location \
      --connect-timeout 15 --max-time 120 --output "$temporary" "$url" &&
      printf '%s  %s\n' "$checksum" "$temporary" | sha256sum --check --status; then
    mv "$temporary" "$destination"
  else
    rm -f "$temporary"
    echo "Failed to obtain checksum-verified TMDB input: $(basename "$destination")" >&2
    return 1
  fi
}

fetch_checked \
  'https://www.kaggle.com/datasets/tmdb/tmdb-movie-metadata/croissant/download' \
  "$fixture_dir/metadata.json" \
  bae45676ddd40273462523ac805bd0219153dd845aeaea2de65279f8b6529ac6

archive_url=$(jq -er \
  '.distribution[] | select(."@id" == "archive.zip") | .contentUrl' \
  "$fixture_dir/metadata.json")
fetch_checked "$archive_url" "$fixture_dir/archive.zip" \
  0342a9748128f44e6a58e96af19d918478af73a8976b142aa4e7e0aa3c50ef1b
