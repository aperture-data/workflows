# Pull base image.
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-object-detection

RUN pip3 install torch torchvision

COPY app /app/
