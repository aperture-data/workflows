# Pull base image with Python and ApertureDB tools
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-postgresql-server
ENV POSTGRES_VERSION=17
ENV PYTHONPATH=/opt/venv/lib/python3.10/site-packages
ARG MULTICORN_VERSION=3.0

# See https://github.com/pgsql-io/multicorn2?tab=readme-ov-file https://multicorn.readthedocs.io/en/latest/implementing-tutorial.html

# Install PostgreSQL server
# Add the PostgreSQL Global Development Group (PGDG) APT repo
RUN apt-get update && apt-get install -y wget gnupg lsb-release \
    && echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
        > /etc/apt/sources.list.d/pgdg.list \
    && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && apt-get update
 
 RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-${POSTGRES_VERSION} \
    postgresql-contrib-${POSTGRES_VERSION} \
    postgresql-plpython3-${POSTGRES_VERSION}

# Install Dependencies for Building the Multicorn2 extension
RUN apt-get install -y build-essential postgresql-server-dev-${POSTGRES_VERSION}
RUN apt-get install -y python3 python3-dev python3-setuptools python3-pip git

COPY sitecustomize.py /opt/venv/lib/python3.10/site-packages/sitecustomize.py

# Download & Compile Multicorn2
RUN wget https://github.com/pgsql-io/multicorn2/archive/refs/tags/v${MULTICORN_VERSION}.tar.gz && \
    tar -xvf v${MULTICORN_VERSION}.tar.gz && \
    cd multicorn2-${MULTICORN_VERSION} && \
    export CFLAGS="-I/opt/venv/include/python3.10" \
    export LDFLAGS="-L/opt/venv/lib" \
    make PYTHON=/opt/venv/bin/python3 && \
    PYTHON_OVERRIDE=/opt/venv/bin/python3 make install

# RUN git clone https://github.com/pgsql-io/multicorn2.git /multicorn2 && \
#     cd /multicorn2 && \
#     pip install . && \
#     make PYTHON=/opt/venv/bin/python3 && make install

COPY python-wrapper /opt/venv/bin
RUN echo "multicorn.python = '/opt/venv/bin/python-wrapper'" >> /etc/postgresql/${POSTGRES_VERSION}/main/postgresql.conf
# RUN echo "multicorn.python = '/opt/venv/bin/python3'" >> /etc/postgresql/${POSTGRES_VERSION}/main/postgresql.conf

# Install the FDW (Foreign Data Wrapper)
COPY fdw /fdw
RUN cd /fdw && pip3 install .


# Copy the application code
COPY app /app/
ENV MULTICORN_PYTHON="/opt/venv/bin/python3"
# ENV LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu


EXPOSE 5432