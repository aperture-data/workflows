#!/bin/bash

set -e # exit on error
set -u # exit on unset variable
set -o pipefail # exit on pipe failure

function uuid() {
    python3 -c 'import uuid; print(uuid.uuid4())'
}

# We expect WF_OUTPUT (or generate a consistent uuid)
# WF_INPUT gets pinned to WF_OUTPUT
WF_OUTPUT=${WF_OUTPUT:-$(uuid)}
echo "WF_OUTPUT: $WF_OUTPUT"
WF_INPUT=$WF_OUTPUT

# Check that WF_TOKEN is set or the last step will fail
if [ -z "$WF_TOKEN" ]; then
    echo "Error: WF_TOKEN is not set"
    exit 1
fi

# Check that WF_DESCRIPTOR_SET is *not* set
if [ -n "${WF_DESCRIPTOR_SET:-}" ]; then
    echo "Error: WF_DESCRIPTOR_SET is set, but it should not be"
    exit 1
fi

with_env_only() {
    local command="$1"
    shift

    # Build environment variable assignments
    local env_args=("PATH=$PATH")  # Include essential vars like PATH
    for var in "$@" ; do
        if [[ -v $var ]]; then
            env_args+=("$var=${!var}")
        fi
    done

    cd /workflows/$command || exit 1
    env -i "${env_args[@]}" bash app.sh
}

with_env_only crawl-website DB_HOST DB_USER DB_PASS WF_START_URLS WF_ALLOWED_DOMAINS WF_OUTPUT WF_CLEAN WF_LOG_LEVEL WF_CONCURRENT_REQUESTS WF_DOWNLOAD_DELAY WF_CONCURRENT_REQUESTS_PER_DOMAIN 

with_env_only text-extraction DB_HOST DB_USER DB_PASS WF_INPUT WF_OUTPUT WF_CLEAN WF_LOG_LEVEL WF_CSS_SELECTOR 

with_env_only text-embeddings DB_HOST DB_USER DB_PASS WF_INPUT WF_OUTPUT WF_CLEAN WF_LOG_LEVEL WF_MODEL WF_ENGINE

with_env_only rag DB_HOST DB_USER DB_PASS WF_INPUT WF_LOG_LEVEL WF_TOKEN WF_LLM_PROVIDER WF_LLM_MODEL WF_LLM_API_KEY WF_MODEL 