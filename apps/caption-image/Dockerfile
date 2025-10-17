# Pull base image.
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-caption-image

COPY requirements.txt /
RUN pip install -U pip
RUN pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu
RUN pip install --no-cache-dir -r /requirements.txt

COPY app/warmup_validate.py /app/warmup_validate.py
RUN python /app/warmup_validate.py

COPY app /app/
