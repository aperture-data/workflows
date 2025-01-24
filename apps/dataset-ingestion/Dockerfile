# Pull base image.
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-dataset-ingestion

RUN apt update && apt-get -y install git bzip2

RUN pip install pycocotools
# This is needed to install dev version of aperturedb.
# We do not really need to install all of that.
RUN apt -y update
RUN apt -y install build-essential pkg-config libfuse-dev fuse python3-dev
RUN pip install "aperturedb[dev]>=0.4.40"

RUN pip install git+https://github.com/openai/CLIP.git

COPY app /app/
