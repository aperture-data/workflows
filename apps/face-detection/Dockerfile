# Pull base image.
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-face-detection
RUN pip install facenet-pytorch

COPY app /app/
