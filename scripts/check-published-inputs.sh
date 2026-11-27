#!/usr/bin/env bash
# Decide which images CI has to build, one image at a time. Each image's input
# hash covers:
#   - the inputs given after --, such as the resolved SDK;
#   - every committed file outside the app folders (apps/*/);
#   - for an app, its folder apps/<app> and the folders of the apps whose
#     <prefix><app> images its Dockerfile builds FROM, recursively.
# The name base stands for the image built from the shared files alone.
# An image is built unless <prefix><name>:<tag> is published with its hash in
# the com.aperturedata.input-hash label. FORCE_BUILD=true builds every image.
# Prints, for $GITHUB_OUTPUT:
#   input-hashes={"<name>":"<hash>",...}
#   builds=["<name>",...]    the images to build, in the order given
#   build=<true|false>       whether any image builds
# Needs git, oras and jq.
#   check-published-inputs.sh <tag> <image prefix> <name>... -- <input>...
set -euo pipefail

tag=$1 prefix=$2
shift 2
names=()
while (( $# )) && [[ $1 != -- ]]; do
    names+=("$1")
    shift
done
(( $# )) && shift
inputs=("$@")

for name in "${names[@]}"; do
    if [[ $name != base ]] && ! git cat-file -e "HEAD:apps/$name" 2>/dev/null; then
        echo "apps/$name is not committed" >&2
        exit 1
    fi
done

# Files directly in apps/, such as build scripts, are shared.
shared=$(git ls-tree HEAD | awk -F'\t' '$2 != "apps"'; git ls-tree HEAD apps/ | awk '$2 == "blob"')

declare -A seen
# Print an app and, recursively, the apps its Dockerfile builds FROM.
app_folders() {
    local app=$1 image dep
    [[ -z ${seen[$app]:-} ]] || return 0
    seen[$app]=1
    printf '%s\n' "$app"
    while read -r image; do
        [[ $image == "$prefix"* ]] || continue
        dep=${image#"$prefix"}
        dep=${dep%%[:@]*}
        if git cat-file -e "HEAD:apps/$dep" 2>/dev/null; then
            app_folders "$dep"
        fi
    done < <(git show "HEAD:apps/$app/Dockerfile" 2>/dev/null \
        | sed -nE 's/^[[:space:]]*FROM[[:space:]]+(--[^[:space:]]+[[:space:]]+)*([^[:space:]]+).*/\2/Ip')
}

input_hash() {
    local name=$1 folder
    {
        printf '%s\n' "$shared"
        if [[ $name != base ]]; then
            while read -r folder; do
                printf 'apps/%s %s\n' "$folder" "$(git rev-parse "HEAD:apps/$folder")"
            done < <(seen=(); app_folders "$name" | sort)
        fi
        printf '%s\n' "${inputs[@]}"
    } | sha256sum | cut -d' ' -f1
}

published_hash() {
    local reference=registry-1.docker.io/$1 config
    if config=$(oras manifest fetch-config --platform linux/amd64 "$reference" 2>&1); then
        jq -r '.config.Labels["com.aperturedata.input-hash"] // ""' <<< "$config"
    elif [[ $config == *"$reference: not found" ]]; then
        return 0
    else
        printf '%s\n' "$config" >&2
        return 1
    fi
}

hashes='{}'
builds=()
for name in "${names[@]}"; do
    hash=$(input_hash "$name")
    hashes=$(jq -c --arg name "$name" --arg hash "$hash" '.[$name] = $hash' <<< "$hashes")
    image=$prefix$name:$tag
    if [[ ${FORCE_BUILD:-false} == true ]]; then
        builds+=("$name")
        continue
    fi
    published=$(published_hash "$image")
    if [[ $published == "$hash" ]]; then
        echo "$image already has input hash $hash" >&2
    else
        echo "$image has input hash '${published}', expected $hash: building" >&2
        builds+=("$name")
    fi
done

echo "input-hashes=$hashes"
echo "builds=$(jq -cn '$ARGS.positional' --args "${builds[@]}")"
if (( ${#builds[@]} )); then
    echo "build=true"
else
    echo "build=false"
fi
