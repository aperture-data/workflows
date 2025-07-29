#!/bin/bash
set -e

DATABASE="aperturedb"

# Check if POSTGRES_PASSWORD is set
if [ -z "$POSTGRES_PASSWORD" ]; then
  echo "Error: POSTGRES_PASSWORD environment variable is not set."
  exit 1
fi

# Start PostgreSQL in the background
echo "Starting PostgreSQL..."
# export PYTHONPATH="/opt/venv/lib/python3.10/site-packages:/app"
/etc/init.d/postgresql start

until pg_isready -U postgres -h /var/run/postgresql ; do
  echo "Waiting for postgres..."
  sleep 1
done

# Set the password for the default 'postgres' user
echo "Setting postgres password..."
su - postgres -c "psql -U postgres -d postgres -c \"ALTER USER postgres WITH PASSWORD '${POSTGRES_PASSWORD}';\""

su - postgres -c "createdb ${DATABASE}"

run_psql() {
  echo "Running SQL command: $1"
  su - postgres -c "psql -d ${DATABASE} -c \"$1\""
}

echo grep multicorn.python /etc/postgresql/17/main/postgresql.conf
grep multicorn.python /etc/postgresql/17/main/postgresql.conf

run_psql "CREATE EXTENSION IF NOT EXISTS plpython3u;"
run_psql "CREATE OR REPLACE FUNCTION get_python_path() 
RETURNS SETOF text 
AS \\\$\\\$
    import sys
    for p in sys.path:
        yield p
\\\$\\\$ LANGUAGE plpython3u;"
run_psql "SELECT * FROM get_python_path();"


run_psql "CREATE EXTENSION IF NOT EXISTS multicorn;"
run_psql "SHOW multicorn.python;"
# /opt/venv/bin/python3 -c "import multicorn; import fdw; print('success')"
run_psql "SHOW config_file;"
run_psql "CREATE SERVER IF NOT EXISTS aperturedb FOREIGN DATA WRAPPER multicorn options (wrapper 'fdw.FDW');"
run_psql "IMPORT FOREIGN SCHEMA ignored_placeholder FROM SERVER aperturedb INTO public;"

echo "Setup complete. Tailing logs to keep container alive..."
tail -f /var/log/postgresql/postgresql-${POSTGRES_VERSION}-main.log