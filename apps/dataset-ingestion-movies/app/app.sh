#!/bin/bash
set -e

python ingest_movies.py --ingest-posters --embed-taglines

adb utils execute summary
