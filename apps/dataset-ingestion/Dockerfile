# Pull base image.
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-dataset-ingestion

RUN apt update && apt-get -y install bzip2

RUN pip install facenet-pytorch --no-deps
RUN pip install pycocotools

COPY app /app/
