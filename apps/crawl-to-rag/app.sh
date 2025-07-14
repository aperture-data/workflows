#!/bin/bash

set -e # exit on error
set -u # exit on unset variable
set -o pipefail # exit on pipe failure

bg_pid="" # track background process ID
NOT_READY_FILE=/workflows/rag/not-ready.txt

function log_status() {
    local message="$1"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $message" >> $NOT_READY_FILE
    echo "*** $message"
}

trap 'log_status "An error occurred at line $LINENO"; exit 1' ERR

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

    echo
    echo "Running $command"

    cd /workflows/$command || exit 1
    env -i "${env_args[@]}" bash app.sh
}

function cleanup() {
    # If background job is still running, kill it
    if [[ -n "$bg_pid" && -e /proc/$bg_pid ]]; then
        echo "Killing background process $bg_pid"
        kill $bg_pid 2>/dev/null || true
        wait $bg_pid 2>/dev/null || true
    fi
}

function set_ready() {
    mv $NOT_READY_FILE $NOT_READY_FILE.bak
}

log_status "Starting crawl-to-rag workflow: $WF_OUTPUT"

COMMON_PARAMETERS="DB_HOST DB_USER DB_PASS APERTUREDB_KEY"

# Run these in a separate thread so we can start the rag server
(
    log_status "Starting crawl"

    with_env_only crawl-website $COMMON_PARAMETERS WF_START_URLS WF_ALLOWED_DOMAINS WF_OUTPUT WF_CLEAN WF_LOG_LEVEL WF_CONCURRENT_REQUESTS WF_DOWNLOAD_DELAY WF_CONCURRENT_REQUESTS_PER_DOMAIN 

    log_status "Crawl complete; starting text-extraction"

    with_env_only text-extraction $COMMON_PARAMETERS WF_INPUT WF_OUTPUT WF_CLEAN WF_LOG_LEVEL WF_CSS_SELECTOR 

    log_status "Text-extraction complete; starting text-embeddings"

    with_env_only text-embeddings $COMMON_PARAMETERS WF_INPUT WF_OUTPUT WF_CLEAN WF_LOG_LEVEL WF_MODEL WF_ENGINE

    log_status "Text-embeddings complete"
    set_ready
)&
bg_pid=$!

# Trap ERR and script exit
trap 'fatal $LINENO' ERR
trap cleanup EXIT

echo "Running webserver for RAG API"

with_env_only rag $COMMON_PARAMETERS WF_INPUT WF_LOG_LEVEL WF_TOKEN WF_LLM_PROVIDER WF_LLM_MODEL WF_LLM_API_KEY WF_MODEL WF_N_DOCUMENTS UVICORN_LOG_LEVEL UVICORN_WORKERS
