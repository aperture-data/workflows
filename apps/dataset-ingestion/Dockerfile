# Pull base image.
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-dataset-ingestion

RUN apt update && apt-get -y install git bzip2


#Install CPU version of torch
RUN pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu
RUN pip install facenet-pytorch --no-deps
RUN pip install pycocotools
RUN pip install git+https://github.com/openai/CLIP.git

COPY app /app/
RUN python3 /app/get_weights.py