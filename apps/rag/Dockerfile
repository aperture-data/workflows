# Pull base image with Python and ApertureDB tools
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-rag

# Install dependencies; git is required for CLIP
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Install dependencies for embeddings
# Install CPU torch first so the embeddings extra does not pull the CUDA build.
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu "torch>=2.0" torchvision
RUN pip install --no-cache-dir "aperturedb-workflows[embeddings]"

# Application requirements moved here so that CPU index is used.
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt


# We prefer to cache models in the docker image rather than load them
# at run time.
COPY app/llm.py /app/llm.py
RUN python3 /app/llm.py
EXPOSE 8000

# Copy the rest of the application code
COPY app /app/


