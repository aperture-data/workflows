# Pull base image.
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-knn

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY app /app/
