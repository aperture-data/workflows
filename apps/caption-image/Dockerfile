# Pull base image.
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-caption-image

COPY requirements.txt /
RUN pip install -U pip
RUN pip install --no-cache-dir -r /requirements.txt

COPY warmup_validate.py /warmup_validate.py
ARG PRELOAD_MODEL=false
RUN if [ "$PRELOAD_MODEL" = "true" ]; then python /warmup_validate.py && rm -rf /root/.cache/huggingface /root/.cache/torch; fi && rm /warmup_validate.py

COPY app /app/
