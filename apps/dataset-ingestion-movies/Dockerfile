# Pull base image.
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=dataset-ingestion-movies

# Install dependencies for embeddings
RUN pip install --no-cache-dir -r /app/embeddings/requirements_cpu.txt
RUN pip install --no-cache-dir -r /app/embeddings/requirements.txt

COPY requirements.txt /
RUN apt-get update && apt-get install -y \
    git \
    git-lfs \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade --no-cache-dir -r /requirements.txt

COPY app /app/
