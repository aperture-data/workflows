# Pull base image.
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-ingest-from-sql

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
RUN apt update && apt install -qy libpq5 libmagic1

COPY app /app/
