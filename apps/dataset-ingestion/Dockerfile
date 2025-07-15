# Pull base image.
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-dataset-ingestion

RUN apt update && apt-get -y install bzip2

RUN pip install facenet-pytorch --no-deps
RUN pip install pycocotools

COPY app /app/
