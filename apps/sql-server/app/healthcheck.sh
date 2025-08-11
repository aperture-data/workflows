#!/bin/bash
set -eu

WF_AUTH_TOKEN=${WF_AUTH_TOKEN:-test}
# echo "Running healthcheck for SQL Server: $(date)"

# Check Postgres server on TCP port 5432
pg_isready -h 127.0.0.1 -p 5432

# Check HTTP API endpoint on port 80
curl -fsS --connect-timeout 2 --max-time 3 \
  -X POST http://127.0.0.1/sql/query \
  -H 'accept: application/json' \
  -H "Authorization: Bearer ${WF_AUTH_TOKEN}" \
  -H 'Content-Type: application/json' \
  --data-raw '{"query":"SELECT 1;"}' \
  | jq -e '(.rows // .data // .result | flatten | first) == 1' >/dev/null

# echo "Healthcheck passed for SQL Server: $(date)"