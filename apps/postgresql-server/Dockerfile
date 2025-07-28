# Pull base image with Python and ApertureDB tools
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-rag
ENV POSTGRES_VERSION=14

# Install PostgreSQL server
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-${POSTGRES_VERSION} \
    postgresql-contrib-${POSTGRES_VERSION}

# Install build dependencies for multicorn2
RUN apt-get install -y --no-install-recommends \
    postgresql-server-dev-${POSTGRES_VERSION} \
    python3-dev \
    build-essential \
    git \
    libpq-dev && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get remove -y postgresql-${POSTGRES_VERSION}-multicorn || true

RUN set -ex  && pg_config --version  # should return "PostgreSQL 14.x"

COPY sitecustomize.py /opt/venv/lib/python3.10/site-packages/sitecustomize.py

ARG MULTICORN_VERSION=v3.0

# Clone and build multicorn2
RUN git clone --branch ${MULTICORN_VERSION} https://github.com/pgsql-io/multicorn2.git /tmp/multicorn2 && \
    cd /tmp/multicorn2 && \
    make PG_CONFIG=/usr/bin/pg_config && \
    make install PG_CONFIG=/usr/bin/pg_config && \
    rm -rf /tmp/multicorn2

COPY python-wrapper /opt/venv/bin
RUN echo "multicorn.python = '/opt/venv/bin/python-wrapper'" >> /etc/postgresql/${POSTGRES_VERSION}/main/postgresql.conf

# Install Python dependencies
# COPY requirements.txt /requirements.txt
# RUN pip install --no-cache-dir -r /requirements.txt
RUN apt-get update && apt-get install -y \
  python3-pip python3-dev libpq-dev build-essential \
  && unset VIRTUAL_ENV \
  && /usr/bin/pip3 install "git+https://github.com/pgsql-io/multicorn2.git"

# Install the FDW (Foreign Data Wrapper)
COPY fdw /fdw
RUN cd /fdw && unset VIRTUAL_ENV && /usr/bin/pip3 install .

# Copy the application code
COPY app /app/
ENV MULTICORN_PYTHON="/opt/venv/bin/python3"
ENV LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu

EXPOSE 5432