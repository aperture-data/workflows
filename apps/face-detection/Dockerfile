# Pull base image.
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-face-detection
RUN pip install facenet-pytorch --no-deps

COPY app /app/
