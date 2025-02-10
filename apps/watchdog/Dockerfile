# Pull base image.
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-watchdog

RUN pip install slack-sdk

COPY app /app/
