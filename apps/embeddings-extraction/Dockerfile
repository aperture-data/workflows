# Pull base image.
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-embeddings-extraction

RUN pip install aperturedb wheel ftfy regex tqdm

RUN apt-get update && apt-get install -y \
    git \
    tesseract-ocr \
    libtesseract-dev \
    && rm -rf /var/lib/apt/lists/*
#Install CPU version of torch

# Install dependencies for embeddings
RUN pip install --no-cache-dir -r /app/embeddings/requirements_cpu.txt
RUN pip install --no-cache-dir -r /app/embeddings/requirements.txt

# Install dependencies for text extraction
RUN pip install --no-cache-dir -r /app/text_extraction/requirements.txt

# Install dependencies for the app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Warm up EasyOCR
COPY app/warm_easyocr.py /app/warm_easyocr.py
RUN python /app/warm_easyocr.py

COPY app /app/
