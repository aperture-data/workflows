# Pull base image.
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-object-detection

RUN pip3 install torch torchvision --index-url https://download.pytorch.org/whl/cpu

COPY app /app/
