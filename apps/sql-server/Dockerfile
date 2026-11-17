# Pull base image with Python and ApertureDB tools
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-sql-server
ENV POSTGRES_VERSION=17
ARG MULTICORN_VERSION=3.2

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
    python3 python3-dev python3-pip build-essential git rpm2cpio cpio

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

# PGDG preserves the upstream release after its GitHub repository disappeared.
# Verify both the official source RPM and its unchanged upstream source archive.
RUN wget --quiet -O /tmp/multicorn2.src.rpm \
      "https://dnf-srpms.postgresql.org/srpms/17/redhat/rhel-9-x86_64/multicorn2_17-${MULTICORN_VERSION}-1PGDG.rhel9.src.rpm" \
 && echo "588387b47ce8ea6211530bdb0a28618463317e5213c8de8b13dea92559d47f75  /tmp/multicorn2.src.rpm" | sha256sum --check \
 && mkdir /multicorn2-source \
 && cd /multicorn2-source \
 && rpm2cpio /tmp/multicorn2.src.rpm | cpio -id --quiet "v${MULTICORN_VERSION}.tar.gz" \
 && echo "e31e8f6e25acafd0dc8a2faf90a8c1e29e114abe8383e5c1041f54ab0f9bb44f  v${MULTICORN_VERSION}.tar.gz" | sha256sum --check \
 && tar -xzf "v${MULTICORN_VERSION}.tar.gz" \
 && cd "multicorn2-${MULTICORN_VERSION}" \
 && /usr/bin/python3 -m pip install --compile --no-cache-dir . \
 && make PG_CONFIG="/usr/lib/postgresql/${POSTGRES_VERSION}/bin/pg_config" PYTHON_OVERRIDE=/usr/bin/python3 \
 && make install PG_CONFIG="/usr/lib/postgresql/${POSTGRES_VERSION}/bin/pg_config" PYTHON_OVERRIDE=/usr/bin/python3 \
 && rm -rf /tmp/multicorn2.src.rpm /multicorn2-source

RUN pip install --no-cache-dir numpy pydantic dotenv typer

# Copy and install our FDW into system Python
COPY fdw /fdw
RUN cd /fdw && /usr/bin/python3 -m pip install --compile .

# Restore virtual environment
ENV VIRTUAL_ENV=${OLD_VIRTUAL_ENV}
ENV PATH="${OLD_PATH}"
ENV PYTHONPATH="/app:${OLD_PYTHONPATH}"

# Install dependencies for embeddings
RUN pip install --no-cache-dir -r /app/embeddings/requirements_cpu.txt
RUN pip install --no-cache-dir -r /app/embeddings/requirements.txt

# Install application requirements
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy the application code
COPY app /app/

EXPOSE 5432
ENV PGUSER=postgres
