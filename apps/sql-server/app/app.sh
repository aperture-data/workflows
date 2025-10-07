#!/bin/bash
set -e

# Dump log file on error
trap 'echo "An error occurred. Check the logs for details."; cat /tmp/fdw.log' ERR

WF_LOG_LEVEL=${WF_LOG_LEVEL:-WARNING}
echo "WF_LOG_LEVEL=$WF_LOG_LEVEL" >>/app/aperturedb.env

# Start proxy server
echo "Starting ApertureDB proxy server..."
SOCK=/tmp/aperturedb-proxy.sock
if [ -S "$SOCK" ]; then
  echo "Removing existing socket file at $SOCK"
  rm -f "$SOCK"
fi

# Uvicorn likes lower-case log levels
UVICORN_LOG_LEVEL=${WF_LOG_LEVEL,,}
uvicorn proxy:app --uds "$SOCK" --log-level ${UVICORN_LOG_LEVEL} &
PROXY_PID=$!


# Check if WF_AUTH_TOKEN is set
if [ -z "$WF_AUTH_TOKEN" ]; then
  echo "Error: WF_AUTH_TOKEN environment variable is not set."
  exit 1
fi

SQL_NAME=${SQL_NAME:-aperturedb}
SQL_USER=${SQL_USER:-aperturedb}
SQL_PASS=${WF_AUTH_TOKEN:-test}
SQL_PORT=${SQL_PORT:-5432}

# Make Postgres use the provided certificates if they exist and are readable by the current user
CERTS_DIR="/etc/tls/certs"
if [ -r $CERTS_DIR/tls.crt ] && [ -r $CERTS_DIR/tls.key ]; then
  echo "Using provided TLS certificates for PostgreSQL."
  cat <<EOF >>/etc/postgresql/${POSTGRES_VERSION}/main/postgresql.conf
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
su - postgres -c "psql <<'EOF'
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${SQL_USER}') THEN
    EXECUTE format(
      'CREATE ROLE ${SQL_USER} LOGIN PASSWORD %L',
      '${SQL_PASS}'
    );
  END IF;
END;
\$\$;
EOF"
su - postgres -c "set -e ; createdb ${SQL_NAME}"


# Check that we've created the user and database
# echo "Checking user exists..."
# su - postgres -c "psql -h 127.0.0.1 -p 5432 -U postgres -tAc \"SELECT rolname FROM pg_roles WHERE rolname='${SQL_USER}';\" "
echo "Checking database exists and user can access it..."
(PGPASSWORD="${SQL_PASS}" psql --host=127.0.0.1 --port=${SQL_PORT} --username=$SQL_USER --dbname=${SQL_NAME} --tuples-only --no-align --command="SELECT 1;" | grep -q 1 ) || exit 1

function psql_load_sql() {
  local file=$1
  echo "Loading SQL file: $file"
  # If file starts with a slash, absolute path, otherwise relative to /app/sql
  if [[ $file != /* ]]; then
    file="/app/sql/$file"
  fi
  set +e
  su - postgres -c "set -e ; psql --echo-all --set ON_ERROR_STOP=on --username postgres --dbname ${SQL_NAME} --file $file --single-transaction"
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

mkdir /app/sql/annotations
chmod 777 /app/sql/annotations

psql_load_sql "types.sql"
psql_load_sql "import.sql"
# DEBUG CODE: DO NOT COMMIT
su - postgres -c "psql -U postgres -d \"$SQL_NAME\" -c '\det connection.*'"
psql_load_sql "functions.sql"
psql_load_sql "access.sql"

for file in /app/sql/annotations/*.sql; do
  psql_load_sql "$file"
done

echo "Setup complete. Tailing logs to keep container alive..."
# Start tailing logs in the background
tail -n 1000 -f /var/log/postgresql/postgresql-${POSTGRES_VERSION}-main.log /tmp/fdw.log &
TAIL_PID=$!

PORT=80
# Run app.py with uvicorn, exit if it crashes
echo "Starting FastAPI server..."
uvicorn app:app \
  --host 0.0.0.0 \
  --port $PORT \
  --workers ${UVICORN_WORKERS:-1} \
  --log-level ${UVICORN_LOG_LEVEL:-info}
UVICORN_STATUS=$?

# If uvicorn exits, kill the tail process and exit with uvicorn's status
kill $TAIL_PID
wait $TAIL_PID 2>/dev/null

kill $PROXY_PID
wait $PROXY_PID 2>/dev/null

exit $UVICORN_STATUS
