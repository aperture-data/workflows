# Pull base image.
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-nsfw-detection

COPY app/requirements.txt /app/requirements.txt
RUN pip3 install -r /app/requirements.txt

COPY app /app/
