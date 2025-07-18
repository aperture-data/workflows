# Pull base image.
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-text-extraction
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
# Pre-load the models for ChatGPT-style tokenization; avoid run-time dependency on external servers
RUN python3 -c "import tiktoken; [tiktoken.encoding_for_model(m).encode('Hello world') for m in ['gpt-3.5-turbo', 'gpt-4']]"

COPY app /app/
