#!/bin/bash
set -eu -o pipefail

trap 'error on line $LINENO' ERR

SQL_HOST=127.0.0.1
SQL_PORT=5432
SQL_NAME=aperturedb
SQL_USER=aperturedb
SQL_PASS=$WF_AUTH_TOKEN

curl -fsS --unix-socket /tmp/aperturedb-proxy.sock \
  -F 'query=[{"GetStatus": {}}];type=application/json' \
  http://127.0.0.1/aperturedb
echo "ApertureDB proxy is ready"

# Check Postgres server on TCP port 5432
pg_isready --host $SQL_HOST --port $SQL_PORT
echo "Postgres is ready on $SQL_HOST:$SQL_PORT"

# Check HTTP API endpoint on port 80
curl -fsS --connect-timeout 2 --max-time 3 \
  -X POST http://$SQL_HOST/sql/query \
  -H 'accept: application/json' \
  -H "Authorization: Bearer ${SQL_PASS}" \
  -H 'Content-Type: application/json' \
  --data-raw '{"query":"SELECT 1;"}' \
  | jq -e '(.rows // .data // .result | flatten | first) == 1' >/dev/null
echo "HTTP API is ready on $SQL_HOST:80"

PGPASSWORD="${SQL_PASS}" psql --host $SQL_HOST --port $SQL_PORT --username $SQL_USER --dbname $SQL_NAME --command 'SELECT _uniqueid FROM system."Entity" LIMIT 1;'
echo "Entity table is accessible by user $SQL_USER"

echo "Healthcheck passed for SQL Server: $(date)"