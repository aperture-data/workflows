# Pull base image.
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-embeddings-extraction

RUN pip install aperturedb wheel ftfy regex tqdm

RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install git+https://github.com/openai/CLIP.git

COPY app /app/
