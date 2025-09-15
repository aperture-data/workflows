# Pull base image.
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-caption-image

COPY requirements.txt /
RUN pip install -U pip
RUN pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu
RUN pip install --no-cache-dir -r /requirements.txt

COPY app/weights.py /app/weights.py
RUN python /app/weights.py

COPY app /app/
