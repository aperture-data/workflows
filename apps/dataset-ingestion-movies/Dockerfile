# Pull base image.
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-dataset-ingestion-movies

# Install dependencies for embeddings
# Install CPU torch first so the embeddings extra does not pull the CUDA build.
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu "torch>=2.0" torchvision
RUN pip install --no-cache-dir "aperturedb-workflows[embeddings]"

COPY requirements.txt /
RUN apt-get update && apt-get install -y \
    git \
    git-lfs \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade --no-cache-dir -r /requirements.txt

COPY app /app/
