# Pull base image.
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-face-detection
#Install CPU version of torch
RUN pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu
RUN pip install facenet-pytorch --no-deps

COPY app /app/
