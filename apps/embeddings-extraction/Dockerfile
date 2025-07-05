# Pull base image.
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-embeddings-extraction

RUN pip install aperturedb wheel ftfy regex tqdm

COPY app /app/
