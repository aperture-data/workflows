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

RUN set -ex  && pg_config --version  # should return "PostgreSQL 14.x"

ARG MULTICORN_VERSION=v3.0

# Clone and build multicorn2
RUN git clone --branch ${MULTICORN_VERSION} https://github.com/pgsql-io/multicorn2.git /tmp/multicorn2 && \
    cd /tmp/multicorn2 && \
    make PG_CONFIG=/usr/bin/pg_config && \
    make install PG_CONFIG=/usr/bin/pg_config && \
    rm -rf /tmp/multicorn2

RUN echo 'export PYTHONPATH=/app' > /etc/profile.d/postgres-pyenv.sh
RUN echo "multicorn.python = '/usr/bin/python3'" >> /etc/postgresql/14/main/postgresql.conf

# Install special Python dependencies; must bypsss normal venv
# RUN apt-get update && apt-get install -y python3-pip
# COPY requirements.txt /requirements.txt
# RUN /usr/bin/python3 -m pip install --no-cache-dir -r /requirements.txt

# Ensure system Python and pip are available
RUN apt-get update && apt-get install -y python3-pip git

# Install multicorn2 system-wide (outside any venv)
RUN /usr/bin/python3 -m pip install --no-cache-dir git+https://github.com/pgsql-io/multicorn2.git@${MULTICORN_VERSION}
# RUN /usr/bin/python3 -c "import multicorn; print(multicorn.__file__)"

# Copy the application code
COPY app /app/
ENV PYTHONPATH="/app:$PYTHONPATH"
ENV LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu

EXPOSE 5432