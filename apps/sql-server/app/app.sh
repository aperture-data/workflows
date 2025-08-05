#!/bin/bash
set -e

# Dump log file on error
trap 'echo "An error occurred. Check the logs for details."; cat /tmp/fdw.log' ERR

DATABASE="aperturedb"

# Multicorn runs in a secure Python without access to environment variables
# This will allow it to access the ApertureDB instance.
APERTUREDB_KEY=$(python -c "from aperturedb.CommonLibrary import create_connector; c = create_connector(); print(c.config.deflate())")
if [ $? -ne 0 ] || [ -z "$APERTUREDB_KEY" ]; then
  echo "Error: Failed to generate APERTUREDB_KEY using Python command."
  exit 1
fi
echo "APERTUREDB_KEY=$APERTUREDB_KEY" >/app/aperturedb.env

WF_LOG_LEVEL=${WF_LOG_LEVEL:-WARN}
echo "WF_LOG_LEVEL=$WF_LOG_LEVEL" >>/app/aperturedb.env

# Check if WF_AUTH_TOKEN is set
if [ -z "$WF_AUTH_TOKEN" ]; then
  echo "Error: WF_AUTH_TOKEN environment variable is not set."
  exit 1
fi

# Start PostgreSQL in the background
echo "Starting PostgreSQL..."
/etc/init.d/postgresql start

until pg_isready -U postgres -h /var/run/postgresql ; do
  echo "Waiting for postgres..."
  sleep 1
done

# Set the password for the default 'postgres' user
echo "Setting postgres password..."
su - postgres -c "set -e ; psql  --set ON_ERROR_STOP=on --username postgres --dbname postgres --command \"CREATE ROLE aperturedb LOGIN PASSWORD '${WF_AUTH_TOKEN}';\""

su - postgres -c "set -e ; createdb ${DATABASE}"

function psql_load_sql() {
  local file=$1
  echo "Loading SQL file: $file"
  set +e
  su - postgres -c "set -e ; psql --echo-all --set ON_ERROR_STOP=on --username postgres --dbname ${DATABASE} --file /app/sql/$file"
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

exit $UVICORN_STATUS
