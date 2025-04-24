# Pull base image with Python and ApertureDB tools
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-text-embeddings

# Install dependencies
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# We prefer to cache models in the docker image rather than load them
# at run time. 
COPY cache_warmer.py /
RUN python3 /cache_warmer.py

# Copy the application code
COPY app /app/


