# Pull base image.
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-embeddings-extraction

RUN pip install wheel ftfy regex tqdm

# Install dependencies for embeddings
# Install CPU torch first so the embeddings extra does not pull the CUDA build.
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu "torch>=2.0" torchvision
RUN pip install --no-cache-dir "aperturedb-workflows[embeddings]"

# Install dependencies for text extraction
RUN pip install --no-cache-dir "aperturedb-workflows[text]"

# Install dependencies for the app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
RUN python -c "import clip; clip.load('ViT-B/16')"

# PDF segmentation must not fetch tokenizer assets during a workflow run.
ENV TIKTOKEN_CACHE_DIR=/opt/tiktoken-cache
RUN mkdir -p "$TIKTOKEN_CACHE_DIR" && \
    python -c "from aperturedb_workflows.text_extraction.segmentation import TextSegmenter; TextSegmenter(max_tokens=100, overlap_tokens=10)" && \
    chmod -R a+rX "$TIKTOKEN_CACHE_DIR"

COPY app /app/
