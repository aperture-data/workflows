# Pull base image with Python and ApertureDB tools
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-text-embeddings

# Install dependencies for embeddings
RUN pip install --no-cache-dir -r /app/embeddings/requirements_cpu.txt
RUN pip install --no-cache-dir -r /app/embeddings/requirements.txt

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# Copy the rest of the application code
COPY app /app/


