#!/bin/bash

set -e # exit on error
set -u # exit on unset variable
set -o pipefail # exit on pipe failure

NOT_READY_FILE=/workflows/rag/not-ready.txt
STATUS_SCRIPT="/app/status_tools.py"

python $STATUS_SCRIPT --completed 0 \
      --phases rag \
      --phases preprocessing \
      --phase preprocessing

function log_status() {
    local message="$1"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $message" >> $NOT_READY_FILE
    echo "*** $message"
}

function uuid() {
    python3 -c 'import uuid; print(uuid.uuid4())'
}

# We expect WF_OUTPUT (or generate a consistent uuid)
# WF_INPUT gets pinned to WF_OUTPUT
WF_OUTPUT=$(/app/wf_argparse.py --type string --envar WF_OUTPUT --default $(uuid))
echo "WF_OUTPUT: $WF_OUTPUT"
WF_INPUT=$WF_OUTPUT

WF_TOKEN=$(/app/wf_argparse.py --type string --envar WF_TOKEN)

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
    env -i "${env_args[@]}" bash -e app.sh
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

COMMON_PARAMETERS="DB_HOST DB_HOST_PUBLIC DB_HOST_PRIVATE_TCP DB_HOST_PRIVATE_HTTP HOSTNAME DB_USER DB_PASS APERTUREDB_KEY"



# Run these in a separate thread so we can start the rag server
(
    log_status "Starting crawl"

    with_env_only crawl-website $COMMON_PARAMETERS WF_START_URLS WF_ALLOWED_DOMAINS WF_OUTPUT WF_CLEAN WF_LOG_LEVEL WF_CONCURRENT_REQUESTS WF_DOWNLOAD_DELAY WF_CONCURRENT_REQUESTS_PER_DOMAIN PYTHONPATH

    log_status "Crawl complete; starting text-extraction"

    with_env_only text-extraction $COMMON_PARAMETERS WF_INPUT WF_OUTPUT WF_CLEAN WF_LOG_LEVEL WF_CSS_SELECTOR PYTHONPATH

    log_status "Text-extraction complete; starting text-embeddings"

    with_env_only text-embeddings $COMMON_PARAMETERS WF_INPUT WF_OUTPUT WF_CLEAN WF_LOG_LEVEL WF_MODEL WF_ENGINE PYTHONPATH

    log_status "Text-embeddings complete"
    set_ready
)&
pipeline_pid=$!

set -m
(
    echo "Running webserver for RAG API"

    with_env_only rag $COMMON_PARAMETERS WF_INPUT WF_LOG_LEVEL WF_TOKEN WF_LLM_PROVIDER WF_LLM_MODEL WF_LLM_API_KEY WF_N_DOCUMENTS UVICORN_LOG_LEVEL UVICORN_WORKERS PYTHONPATH WF_ALLOWED_ORIGINS
)&
set +m
server_pid=$!

# Wait for pipeline to complete
set +e
wait $pipeline_pid
pipeline_status=$?
set -e

echo "Pipeline completed with status $pipeline_status"

if [ $pipeline_status -ne 0 ]; then
    kill -9 -$server_pid
fi

# Wait on server forever
wait $server_pid