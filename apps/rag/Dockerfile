# Pull base image with Python and ApertureDB tools
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-rag
ENV POSTGRES_PASSWORD=test

# Install PostgreSQL
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql postgresql-contrib && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r /requirements.txt

RUN service postgresql start && \
    su - postgres -c "psql -c \"ALTER USER postgres WITH PASSWORD '${POSTGRES_PASSWORD}';\"" && \
    echo "host all  all    0.0.0.0/0  md5" >> /etc/postgresql/*/main/pg_hba.conf && \
    echo "listen_addresses='*'" >> /etc/postgresql/*/main/postgresql.conf

EXPOSE 5432

# Copy the rest of the application code
COPY app /app/


