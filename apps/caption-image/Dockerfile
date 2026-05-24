# Pull base image.
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-caption-image

COPY requirements.txt /
RUN pip install -U pip
RUN pip install --no-cache-dir -r /requirements.txt

COPY app/warmup_validate.py /app/warmup_validate.py
ARG PRELOAD_MODEL=false
RUN if [ "$PRELOAD_MODEL" = "true" ]; then python /app/warmup_validate.py; fi

COPY app /app/
