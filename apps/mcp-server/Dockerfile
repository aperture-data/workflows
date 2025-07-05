# Pull base image with Python and ApertureDB tools
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-mcp-server

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

EXPOSE 8000

# Copy the rest of the application code
COPY app /app/


