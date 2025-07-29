# Pull base image.
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-ingest-from-sql

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
RUN apt update && apt install libpq5

COPY app /app/
