#!/bin/bash
set -e

DATABASE="aperturedb"

# Copy selected environment variables to a file
printenv | grep DB_ > /app/aperturedb.env

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
su - postgres -c "psql -U postgres -d postgres -c \"ALTER USER postgres WITH PASSWORD '${WF_AUTH_TOKEN}';\""

su - postgres -c "createdb ${DATABASE}"

su - postgres -c "psql -a -d ${DATABASE} -f /app/init.sql"

echo "Setup complete. Tailing logs to keep container alive..."
tail -f /var/log/postgresql/postgresql-${POSTGRES_VERSION}-main.log /tmp/fdw.log