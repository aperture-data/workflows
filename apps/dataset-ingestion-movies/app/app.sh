#!/bin/bash
set -e

python ingest_movies.py

adb utils execute summary
