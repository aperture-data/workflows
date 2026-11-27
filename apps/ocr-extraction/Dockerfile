# Pull base image.
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-ocr-extraction

RUN pip install wheel ftfy regex tqdm

RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    libtesseract-dev \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies for embeddings
# Install CPU torch first so the embeddings extra does not pull the CUDA build.
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu "torch>=2.0" torchvision
RUN pip install --no-cache-dir "aperturedb-workflows[embeddings]"

# Install dependencies for text extraction
RUN pip install --no-cache-dir "aperturedb-workflows[text]"

# Install dependencies for the app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Warm up EasyOCR
COPY app/warm_easyocr.py /app/warm_easyocr.py
RUN python /app/warm_easyocr.py

COPY app /app/
