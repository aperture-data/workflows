#!/bin/bash
#
# This script is used to run the pre-rag workflow
# It runs the following steps:
# 1. Crawl the website
# 2. Extract text from the crawled website
# 3. Embed the extracted text


set -x
set -euo pipefail

# These parameters should be overridden by the user
# START

DB_HOST=${DB_HOST:-"localhost"}
DB_USER=${DB_USER:-"admin"}
DB_PASS=${DB_PASS:-"admin"}

ID=${ID:-"my_key"}
SECRET=${SECRET:-"my_secret"}
START_URLS=${START_URLS:-"https://aperturedata.io/"}
ALLOWED_DOMAINS=${ALLOWED_DOMAINS:-"docs.aperturedata.io"}

# END

RUNNER_NAME="$(whoami)"

CRAWL_IMAGE="aperturedata/workflows-crawl-website"
CRAWL_NAME="${RUNNER_NAME}_crawl_website"

EXTRACT_IMAGE="aperturedata/workflows-text-extraction"
EXTRACT_NAME="${RUNNER_NAME}_text_extraction"

EMBED_IMAGE="aperturedata/workflows-text-embeddings"
EMBED_NAME="${RUNNER_NAME}_text_embeddings"


CLEANUP=${CLEANUP:-true}

function run_crawl() {
    # Run until complete
    docker run \
               --name ${CRAWL_NAME} \
               -e DB_HOST=${DB_HOST} \
               -e DB_USER=${DB_USER} \
               -e DB_PASS=${DB_PASS} \
               -e WF_START_URLS=${START_URLS} \
               -e WF_ALLOWED_DOMAINS=${ALLOWED_DOMAINS} \
               -e WF_OUTPUT=${ID} \
               -e WF_CLEAN=1 \
               --rm \
               ${CRAWL_IMAGE}
}

function run_extract() {
    # Run until complete
    docker run \
               --name ${EXTRACT_NAME} \
               -e DB_HOST=${DB_HOST} \
               -e DB_USER=${DB_USER} \
               -e DB_PASS=${DB_PASS} \
               -e WF_INPUT=${ID} \
               -e WF_OUTPUT=${ID} \
               -e WF_CLEAN=1 \
               --rm \
               ${EXTRACT_IMAGE}
}

function run_embed() {
    # Run until complete
    docker run \
               --name ${EMBED_NAME} \
               -e DB_HOST=${DB_HOST} \
               -e DB_USER=${DB_USER} \
               -e DB_PASS=${DB_PASS} \
               -e WF_INPUT=${ID} \
               -e WF_OUTPUT=${ID} \
               -e WF_CLEAN=1 \
               --rm \
               ${EMBED_IMAGE}
}

run_crawl
run_extract
run_embed
