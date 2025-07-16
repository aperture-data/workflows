# Pull base image.
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-ingest-croissant

COPY requirements.txt /
RUN apt-get update && apt-get install -y \
    git-lfs \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade --no-cache-dir -r /requirements.txt

COPY app /app/
