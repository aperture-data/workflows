# Pull base image with Python and ApertureDB tools
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-mcp-server

# Install dependencies; git is required for CLIP
# RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

EXPOSE 8002

# Copy the rest of the application code
COPY app /app/


