# Pull base image.
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-ingest-croissant

COPY requirements.txt /
RUN apt-get update && apt-get install -y \
    git \
    git-lfs \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade --no-cache-dir -r /requirements.txt

COPY app /app/
