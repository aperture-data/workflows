# Pull base image with Python and ApertureDB tools
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-rag

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# We prefer to cache models in the docker image rather than load them
# at run time. 
COPY app/llm.py /app/llm.py
RUN python3 /app/llm.py
EXPOSE 8000

# Copy the rest of the application code
COPY app /app/


