# Pull base image.
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-croissant-ingestion




COPY requirements.txt /
RUN apt-get update && apt-get install -y \
    git \
    python3.10-venv \
    && rm -rf /var/lib/apt/lists/*

ENV VIRTUAL_ENV="/app/venv"
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"


RUN pip install --upgrade --no-cache-dir -r /requirements.txt

COPY app /app/
