# Pull base image.
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-dataset-ingestion

RUN apt update && apt-get -y install git bzip2

RUN pip install git+https://github.com/openai/CLIP.git
RUN pip install facenet-pytorch pycocotools

COPY app /app/
