#!/bin/bash
set -eux -o pipefail

trap 'error on line $LINENO' ERR

SQL_HOST=127.0.0.1
SQL_PORT=5432
SQL_NAME=aperturedb
SQL_USER=aperturedb
SQL_PASS=$WF_AUTH_TOKEN
# echo "Running healthcheck for SQL Server: $(date)"

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

PGPASSWORD="${SQL_PASS}" psql --host $SQL_HOST --port $SQL_PORT --username $SQL_USER --dbname $SQL_NAME --command '\dt *.*'
echo "Database $SQL_NAME is accessible by user $SQL_USER"

PGPASSWORD="${SQL_PASS}" psql --host $SQL_HOST --port $SQL_PORT --username $SQL_USER --dbname $SQL_NAME --command '\des+'
PGPASSWORD="${SQL_PASS}" psql --host $SQL_HOST --port $SQL_PORT --username $SQL_USER --dbname $SQL_NAME --command '\dew+'
PGPASSWORD="${SQL_PASS}" psql --host $SQL_HOST --port $SQL_PORT --username $SQL_USER --dbname $SQL_NAME --command '\det+ *.*'

PGPASSWORD="${SQL_PASS}" psql --host $SQL_HOST --port $SQL_PORT --username $SQL_USER --dbname $SQL_NAME --command 'SELECT _uniqueid FROM system."Entity" LIMIT 1;'

PGPASSWORD="${SQL_PASS}" psql --host $SQL_HOST --port $SQL_PORT --username $SQL_USER --dbname $SQL_NAME --command 'SELECT _uniqueid FROM entity."TestRow" LIMIT 1;'

echo "Entity table is accessible by user $SQL_USER"

echo "Healthcheck passed for SQL Server: $(date)"