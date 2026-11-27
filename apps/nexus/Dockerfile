# Pull base image with Python and ApertureDB tools
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-nexus

# CPU-only PyTorch for the sentence-transformers text embedder.
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu torch

# Install Nexus from the source copied into nexus/ (see sync-nexus.sh).
# Passing the base image's SDK wheel keeps pip from taking aperturedb from PyPI.
COPY nexus /opt/aperture-nexus
COPY requirements.txt /
RUN pip install --no-cache-dir /opt/aperturedb-sdk/aperturedb-*-py3-none-any.whl \
        "/opt/aperture-nexus[sentence-transformers]" -r /requirements.txt

# Download the configured embedding model at build time so the server starts offline.
ENV HF_HOME=/opt/huggingface \
    APERTURE_NEXUS_CONFIG=/app/aperture_nexus.json
COPY app/aperture_nexus.json /app/
RUN python -c "import json; from sentence_transformers import SentenceTransformer; \
SentenceTransformer(json.load(open('/app/aperture_nexus.json'))['models']['text_embedding'])"
ENV HF_HUB_OFFLINE=1

EXPOSE 8000

# Copy the rest of the application code
COPY app /app/
