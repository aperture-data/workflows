#!/bin/bash
set -x
set -euo pipefail

RUNNER_NAME="$(whoami)"
NETWORK_NAME="${RUNNER_NAME}_aperturedb"

APERTUREDB_IMAGE="aperturedata/aperturedb-community"
APERTUREDB_NAME="${RUNNER_NAME}_aperturedb"

CRAWL_IMAGE="aperturedata/workflows-crawl-website"
CRAWL_NAME="${RUNNER_NAME}_crawl_website"

EXTRACT_IMAGE="aperturedata/workflows-text-extraction"
EXTRACT_NAME="${RUNNER_NAME}_text_extraction"

EMBED_IMAGE="aperturedata/workflows-text-embeddings"
EMBED_NAME="${RUNNER_NAME}_text_embeddings"

RAG_IMAGE="aperturedata/workflows-rag"
RAG_NAME="${RUNNER_NAME}_rag"

ID="my_key"
SECRET="my_secret"
CRAWL_URL="https://www.aperturedata.io/"
ADB_CONFIG="TEMPORARY_CONFIG_USED_BY_TESTS"

CLEANUP=${CLEANUP:-true}

function build() {
    # Get the directory this script is in
    DIR=$(dirname $(readlink -f $0))
    cd $DIR
    bash ../build.sh
    bash ../build.sh crawl-website
    bash ../build.sh text-extraction
    bash ../build.sh text-embeddings
}

function cleanup() {
    docker stop ${APERTUREDB_NAME} || true
    docker rm ${APERTUREDB_NAME} || true
    docker stop ${CRAWL_NAME} || true
    docker rm ${CRAWL_NAME} || true
    docker stop ${EXTRACT_NAME} || true
    docker rm ${EXTRACT_NAME} || true
    docker stop ${EMBED_NAME} || true
    docker rm ${EMBED_NAME} || true
    docker stop ${RAG_NAME} || true
    docker rm ${RAG_NAME} || true
    docker network rm ${NETWORK_NAME} || true
}

function setup() {
    docker network create ${NETWORK_NAME}

    docker build -t get_summary -f- . <<EOD
FROM aperturedata/workflows-base
RUN echo "python3 -c 'from aperturedb.CommonLibrary import create_connector; client = create_connector(); from aperturedb.Utils import Utils; utils = Utils(client); import json; print(json.dumps(utils.get_schema(), indent=4))'" > /app/app.sh
EOD
}

function run_aperturedb() {
    # Run as a daemon
    docker run -d \
               --name ${APERTUREDB_NAME} \
               --network ${NETWORK_NAME} \
               -e ADB_MASTER_KEY="admin" \
               -e ADB_KVGD_DB_SIZE="204800" \
               ${APERTUREDB_IMAGE}
    # Wait for the database to start
    sleep 20
}

function run_crawl() {
    # Run until complete
    docker run \
               --name ${CRAWL_NAME} \
               --network ${NETWORK_NAME} \
               -e DB_HOST=${APERTUREDB_NAME} \
               -e WF_START_URLS=${CRAWL_URL} \
               -e WF_MAX_DOCUMENTS=20 \
               -e WF_OUTPUT=${ID} \
               --rm \
               ${CRAWL_IMAGE}
}

function run_extract() {
    # Run until complete
    docker run \
               --name ${EXTRACT_NAME} \
               --network ${NETWORK_NAME} \
               -e DB_HOST=${APERTUREDB_NAME} \
               -e WF_INPUT=${ID} \
               -e WF_OUTPUT=${ID} \
               --rm \
               ${EXTRACT_IMAGE}
}

function run_embed() {
    # Run until complete
    docker run \
               --name ${EMBED_NAME} \
               --network ${NETWORK_NAME} \
               -e DB_HOST=${APERTUREDB_NAME} \
               -e WF_INPUT=${ID} \
               -e WF_OUTPUT=${ID} \
               --rm \
               ${EMBED_IMAGE}
}

function get_summary() {
    docker run -t \
        --rm \
        --network ${NETWORK_NAME} \
        -e DB_HOST=${APERTUREDB_NAME} \
        get_summary
}

function run_rag() {
    # Run as a daemon
    docker run -d \
               --name ${RAG_NAME} \
               --network ${NETWORK_NAME} \
               -e DB_HOST=${APERTUREDB_NAME} \
               -e WF_INPUT=${ID} \
               -e WF_TOKEN=${SECRET} \
               --rm \
               ${RAG_IMAGE}
}

if [ "$CLEANUP" = true ]; then
    trap cleanup EXIT
fi

build
cleanup
setup
run_aperturedb
get_summary
run_crawl
get_summary
run_extract
get_summary
run_embed
get_summary
run_rag

sleep 20

if [ "$CLEANUP" = true ]; then
    cleanup
fi

exit 0
