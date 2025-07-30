#!/bin/bash
set -e

DATABASE="aperturedb"

# Copy selected environment variables to a file
printenv | grep DB_ > /app/aperturedb.env

# Check if POSTGRES_PASSWORD is set
if [ -z "$POSTGRES_PASSWORD" ]; then
  echo "Error: POSTGRES_PASSWORD environment variable is not set."
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
su - postgres -c "psql -U postgres -d postgres -c \"ALTER USER postgres WITH PASSWORD '${POSTGRES_PASSWORD}';\""

su - postgres -c "createdb ${DATABASE}"

run_psql() {
  echo "Running SQL command: $1"
  su - postgres -c "psql -d ${DATABASE} -c \"$1\""
}


run_psql "CREATE EXTENSION IF NOT EXISTS multicorn;"
run_psql "CREATE SERVER IF NOT EXISTS aperturedb FOREIGN DATA WRAPPER multicorn options (wrapper 'fdw.FDW');"
run_psql "IMPORT FOREIGN SCHEMA ignored_placeholder FROM SERVER aperturedb INTO public;"

echo "Setup complete. Tailing logs to keep container alive..."
tail -f /var/log/postgresql/postgresql-${POSTGRES_VERSION}-main.log /tmp/fdw.log