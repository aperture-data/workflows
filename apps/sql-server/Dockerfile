# Pull base image with Python and ApertureDB tools
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-sql-server
ENV POSTGRES_VERSION=17
ARG MULTICORN_VERSION=3.0

# Add PGDG repository and install PostgreSQL 
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
# Store current VIRTUAL_ENV and PATH values
ENV OLD_VIRTUAL_ENV="${VIRTUAL_ENV}"
ENV OLD_PATH="${PATH}"
ENV OLD_PYTHONPATH="${PYTHONPATH}"

# Disable virtual environment
ENV VIRTUAL_ENV=
ENV PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
ENV PYTHONPATH=""

# Install multicorn2 Python module into system Python
RUN /usr/bin/python3 -m pip install --compile --no-cache-dir "git+https://github.com/pgsql-io/multicorn2.git"

# Build and install multicorn2 Postgres extension 
RUN git clone --single-branch --branch main https://github.com/pgsql-io/multicorn2.git /multicorn2 \
 && cd /multicorn2 \
 && make PYTHON=/usr/bin/python3 \
 && make install

RUN pip install --no-cache-dir dotenv numpy pydantic aperturedb

# Install dependencies for embeddings
RUN pip install --no-cache-dir -r /app/embeddings/requirements_cpu.txt
RUN pip install --no-cache-dir -r /app/embeddings/requirements.txt

# Copy and install our FDW into system Python
COPY fdw /fdw
RUN cd /fdw && /usr/bin/python3 -m pip install --compile .
\
    # Restore virtual environment
ENV VIRTUAL_ENV=${OLD_VIRTUAL_ENV}
ENV PATH="${OLD_PATH}"
ENV PYTHONPATH="/app:${OLD_PYTHONPATH}"

# Install application requirements
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy the application code
COPY app /app/

EXPOSE 5432
ENV PGUSER=postgres