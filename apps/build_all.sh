
set -e

# Start by changing to the directory of this script
DIR=$(dirname $(readlink -f $0))
cd $DIR

# We have dependencies between workflows, so we need to build them in a 
# specific order. We have three types of workflows:
# 1. Base workflow: This is the base image that all other workflows depend on
# 2. Normal workflows: These workflows depend only on the base workflow
# 3. Meta workflows: These workflows that depend on other (normal) workflows

META_WORKFLOWS=("crawl-to-rag")

ALL_WORKFLOWS=()

# First assemble a list of normal workflows
for d in ../apps/* ; do
  if [ -d "$d" ]; then
    dir_name=$(basename "$d")
    is_meta=false
    for meta in "${META_WORKFLOWS[@]}"; do
      if [ "$dir_name" == "$meta" ]; then
        is_meta=true
        break
      fi
    done
    if ! $is_meta; then
      ALL_WORKFLOWS+=("$dir_name")
    fi
  fi
done
# Now append the meta-workflows to the list
ALL_WORKFLOWS+=("${META_WORKFLOWS[@]}")

echo "Building the base workflow first"
# Build the base workflow first
(cd ../base/docker && ./build.sh)

echo "Building the following workflows in order: ${ALL_WORKFLOWS[@]}"
# Now build everything in order
for name in "${ALL_WORKFLOWS[@]}"; do
  echo "Building $name"
  if [ -d "$name" ]; then
    (cd $name && ../build.sh)
  fi
done
