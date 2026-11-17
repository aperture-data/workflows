# Pull base image.
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-embeddings-extraction

RUN pip install wheel ftfy regex tqdm

# Install dependencies for embeddings
RUN pip install --no-cache-dir -r /app/embeddings/requirements_cpu.txt
RUN pip install --no-cache-dir -r /app/embeddings/requirements.txt

# Install dependencies for text extraction
RUN pip install --no-cache-dir -r /app/text_extraction/requirements.txt

# Install dependencies for the app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
RUN python -c "import clip; clip.load('ViT-B/16')"

# PDF segmentation must not fetch tokenizer assets during a workflow run.
ENV TIKTOKEN_CACHE_DIR=/opt/tiktoken-cache
RUN mkdir -p "$TIKTOKEN_CACHE_DIR" && \
    PYTHONPATH=/app python -c "from text_extraction.segmentation import TextSegmenter; TextSegmenter(max_tokens=100, overlap_tokens=10)" && \
    chmod -R a+rX "$TIKTOKEN_CACHE_DIR"

COPY app /app/
