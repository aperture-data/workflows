# Pull base image with Python and ApertureDB tools
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-text-embeddings

RUN pip install --no-cache-dir -r /app/embeddings/requirements.txt
RUN pip install --no-cache-dir -r /app/embeddings/requirements_cpu.txt

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# We prefer to cache models in the docker image rather than load them
# at run time. 
RUN python3 /app/embeddings/

# Copy the rest of the application code
COPY app /app/


