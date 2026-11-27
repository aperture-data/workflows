# Pull base image with Python and ApertureDB tools
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-mcp-server

# Install dependencies for embeddings
# Install CPU torch first so the embeddings extra does not pull the CUDA build.
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu "torch>=2.0" torchvision
RUN pip install --no-cache-dir "aperturedb-workflows[embeddings]"

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

EXPOSE 8000

# Copy the rest of the application code
COPY app /app/


