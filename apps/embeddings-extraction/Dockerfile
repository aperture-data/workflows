# Pull base image.
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-embeddings-extraction

RUN pip install --no-cache-dir -r /app/embeddings/requirements.txt
RUN pip install --no-cache-dir -r /app/embeddings/requirements_cpu.txt

RUN pip install aperturedb wheel ftfy regex tqdm

COPY app /app/
