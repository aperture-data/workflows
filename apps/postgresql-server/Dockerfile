# Pull base image with Python and ApertureDB tools
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-postgresql-server
ENV POSTGRES_VERSION=17
ARG MULTICORN_VERSION=3.0

# Add PGDG repository and install PostgreSQL 17
RUN apt-get update && apt-get install -y wget gnupg lsb-release \
 && echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
      > /etc/apt/sources.list.d/pgdg.list \
 && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
 && apt-get update

# Install Postgres and Python build tools
RUN apt-get install -y \
    postgresql-${POSTGRES_VERSION} \
    postgresql-contrib-${POSTGRES_VERSION} \
    postgresql-server-dev-${POSTGRES_VERSION} \
    python3 python3-dev python3-pip build-essential git

RUN echo "listen_addresses = '*'" >> /etc/postgresql/${POSTGRES_VERSION}/main/postgresql.conf
RUN echo "host all all 0.0.0.0/0 md5" >> /etc/postgresql/17/main/pg_hba.conf

# Postgres/Multicorn insists on using the system Python, so we need to disable the virtual environment
ENV VIRTUAL_ENV=
ENV PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

# Install multicorn2 Python module into system Python
RUN /usr/bin/python3 -m pip install --no-cache-dir "git+https://github.com/pgsql-io/multicorn2.git"

# Build and install multicorn2 Postgres extension
RUN git clone https://github.com/pgsql-io/multicorn2.git /multicorn2 \
 && cd /multicorn2 \
 && make PYTHON=/usr/bin/python3 \
 && make install

RUN pip install --no-cache-dir aperturedb

# Copy and install our FDW into system Python
COPY fdw /fdw
RUN cd /fdw && /usr/bin/python3 -m pip install .

# Copy the application code
COPY app /app/

# Restore virtual environment
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:/opt/venv/lib/python3.10/site-packages:$PATH"

EXPOSE 5432