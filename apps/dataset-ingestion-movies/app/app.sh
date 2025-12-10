#!/bin/bash
set -e

INGEST_POSTERS=${INGEST_POSTERS:-true}
EMBED_TAGLINE=${EMBED_TAGLINE:-true}

INGEST_POSTERS_COMMAND="--no-ingest-posters"
INGEST_TAGLINE_COMMAND="--no-embed-tagline"

if [ "$INGEST_POSTERS" = "true" ]; then
    INGEST_POSTERS_COMMAND="--ingest-posters"
fi

if [ "$EMBED_TAGLINE" = "true" ]; then
    INGEST_TAGLINE_COMMAND="--embed-tagline"
fi

python ingest_movies.py $INGEST_POSTERS_COMMAND $INGEST_TAGLINE_COMMAND

adb utils execute summary
