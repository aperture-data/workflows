# Pull base image.
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-example

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY app /app/
