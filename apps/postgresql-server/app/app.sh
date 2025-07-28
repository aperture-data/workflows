#!/bin/bash
set -e

DATABASE="aperturedb"

# Check if POSTGRES_PASSWORD is set
if [ -z "$POSTGRES_PASSWORD" ]; then
  echo "Error: POSTGRES_PASSWORD environment variable is not set."
  exit 1
fi

echo "Checking multicorn2 installation..."
/usr/bin/python3 -c "import multicorn; print(multicorn.__file__)"

/usr/bin/python3 -c "import multicorn; print(dir(multicorn))"

# Start PostgreSQL in the background
echo "Starting PostgreSQL..."
/etc/init.d/postgresql start

sleep 2
/usr/bin/python3 -c "import multicorn; import sys; print(multicorn.__file__); print(sys.path)"

# Set the password for the default 'postgres' user
echo "Setting postgres password..."
su - postgres -c "psql -U postgres -d postgres -c \"ALTER USER postgres WITH PASSWORD '${POSTGRES_PASSWORD}';\""

su - postgres -c "createdb ${DATABASE}"

run_psql() {
  echo "Running SQL command: $1"
  su - postgres -c "psql -d ${DATABASE} -c \"$1\""
}

run_psql "CREATE EXTENSION IF NOT EXISTS multicorn;"
# ls $(pg_config --pkglibdir)
# strings $(pg_config --pkglibdir)/multicorn.so | grep -i python | grep lib 
# find / -name libpython3.10.so.1.0 2>/dev/null
# run_psql "SHOW shared_preload_libraries;"
# run_psql "SHOW python_path;"
run_psql "SELECT * FROM pg_extension WHERE extname = 'multicorn';"
run_psql "CREATE SERVER IF NOT EXISTS aperturedb FOREIGN DATA WRAPPER multicorn OPTIONS (wrapper 'fwd.FDW');"
run_psql "IMPORT FOREIGN SCHEMA ignored_placeholder FROM SERVER aperturedb INTO public;"

# Optional: log running databases
echo "Current databases:"
su - postgres -c "psql -U postgres -c \"\\l\""

