# Pull base image.
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-embeddings-extraction

RUN pip install aperturedb wheel ftfy regex tqdm

RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*
#Install CPU version of torch
RUN pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu
RUN pip install git+https://github.com/openai/CLIP.git

RUN pip install --no-cache-dir -r /app/text_extraction/requirements.txt

COPY app /app/
