# Pull base image with Python and ApertureDB tools
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-sparql

# Install dependencies; git is required for CLIP
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

EXPOSE 8001

# Copy the rest of the application code
COPY app /app/


