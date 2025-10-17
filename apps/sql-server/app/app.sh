#!/bin/bash

set -o errexit -o nounset -o pipefail

# Dump log file on error
trap 'echo "An error occurred. Check the logs for details."; cat /tmp/fdw.log' ERR

# Validate and sanitize all external inputs
WF_LOG_LEVEL=$(/app/wf_argparse.py --type log_level --envar WF_LOG_LEVEL --default WARNING)
echo "WF_LOG_LEVEL=$WF_LOG_LEVEL" >>/app/aperturedb.env

WF_AUTH_TOKEN=$(/app/wf_argparse.py --type auth_token --envar WF_AUTH_TOKEN --hidden)
SQL_NAME=$(/app/wf_argparse.py --type sql_identifier --envar SQL_NAME --default aperturedb)
SQL_USER=$(/app/wf_argparse.py --type sql_identifier --envar SQL_USER --default aperturedb)
SQL_PORT=$(/app/wf_argparse.py --type port --envar SQL_PORT --default 5432)
SQL_PASS="${WF_AUTH_TOKEN}"
POSTGRES_VERSION=$(/app/wf_argparse.py --type positive_int --envar POSTGRES_VERSION --default 17)
UVICORN_WORKERS=$(/app/wf_argparse.py --type positive_int --envar UVICORN_WORKERS --default 1)

# Start proxy server
echo "Starting ApertureDB proxy server..."
SOCK=/tmp/aperturedb-proxy.sock
if [ -S "$SOCK" ]; then
  echo "Removing existing socket file at $SOCK"
  rm -f "$SOCK"
fi

# Uvicorn likes lower-case log levels
UVICORN_LOG_LEVEL=${WF_LOG_LEVEL,,}
uvicorn proxy:app --uds "$SOCK" --log-level "${UVICORN_LOG_LEVEL}" &
PROXY_PID=$!

# Make Postgres use the provided certificates if they exist and are readable by the current user
CERTS_DIR="/etc/tls/certs"
if [ -r "$CERTS_DIR/tls.crt" ] && [ -r "$CERTS_DIR/tls.key" ]; then
  echo "Using provided TLS certificates for PostgreSQL."
  cat <<EOF >>"/etc/postgresql/${POSTGRES_VERSION}/main/postgresql.conf"
ssl = on
ssl_cert_file = '$CERTS_DIR/tls.crt'
ssl_key_file = '$CERTS_DIR/tls.key'
EOF
else
  echo "Warning: TLS certificates not found or not readable at $CERTS_DIR/tls.crt and $CERTS_DIR/tls.key. PostgreSQL will not use SSL or will fall back to self-signed mode."
fi

# Start PostgreSQL in the background
echo "Starting PostgreSQL..."
/etc/init.d/postgresql start

until su - postgres -c "pg_isready -h /var/run/postgresql"; do
  echo "Waiting for postgres..."
  sleep 1
done

# Be careful to avoid problems with special characters in the password
DELIM="D$(openssl rand -hex 16)"
(cat <<EOF
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${SQL_USER}') THEN
    EXECUTE format(
      'CREATE ROLE %I LOGIN PASSWORD %L',
      '${SQL_USER}',
      \$${DELIM}\$${SQL_PASS}\$${DELIM}\$
    );
  END IF;
END
\$\$;
EOF
) | su - postgres -s /bin/sh -c 'psql -v ON_ERROR_STOP=1 --pset pager=off'

# Create the database
su - postgres -c "set -e ; createdb \"${SQL_NAME}\""


# Check that we've created the user and database
echo "Checking database exists and user can access it..."
(PGPASSWORD="${SQL_PASS}" psql --host=127.0.0.1 --port=${SQL_PORT} --username="$SQL_USER" --dbname="${SQL_NAME}" --tuples-only --no-align --command="SELECT 1;" | grep -q 1 ) || exit 1

function psql_load_sql() {
  local file=$1
  echo "Loading SQL file: $file"
  set +e
  su - postgres -c "set -e ; psql --echo-all --set ON_ERROR_STOP=on --username postgres --dbname ${SQL_NAME} --file /app/sql/$file"
  if [ $? -ne 0 ]; then
    echo "Failed to load SQL file: $file" >&2
    if [ -f /tmp/fdw.log ]; then
      cat /tmp/fdw.log
    else
      echo "No log file found at /tmp/fdw.log."
    fi
    exit 1
  fi
  set -e
  echo "Successfully loaded SQL file: $file"
}

psql_load_sql "types.sql"
psql_load_sql "import.sql"
psql_load_sql "functions.sql"
psql_load_sql "access.sql"

echo "Setup complete. Tailing logs to keep container alive..."
# Start tailing logs in the background
tail -n 1000 -f "/var/log/postgresql/postgresql-${POSTGRES_VERSION}-main.log" /tmp/fdw.log &
TAIL_PID=$!

PORT=80

# Run app.py with uvicorn, exit if it crashes
echo "Starting FastAPI server..."
uvicorn app:app \
  --host 0.0.0.0 \
  --port "$PORT" \
  --workers "${UVICORN_WORKERS}" \
  --log-level "${UVICORN_LOG_LEVEL}"
UVICORN_STATUS=$?

# If uvicorn exits, kill the tail process and exit with uvicorn's status
kill $TAIL_PID
wait $TAIL_PID 2>/dev/null

kill $PROXY_PID
wait $PROXY_PID 2>/dev/null

exit $UVICORN_STATUS
