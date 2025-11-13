#!/bin/bash
set -e

python ingest_movies.py --ingest-posters --embed-tagline

adb utils execute summary
