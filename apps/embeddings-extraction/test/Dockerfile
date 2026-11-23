ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION} AS workflow_base

FROM python:3.10-slim
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --upgrade pip
COPY requirements.txt .
RUN --mount=type=bind,from=workflow_base,source=/opt/aperturedb-sdk,target=/sdk \
    pip install /sdk/aperturedb-*-py3-none-any.whl -r requirements.txt

# Copy test files and data
COPY images/ /app/images/
COPY pdfs/ /app/pdfs/
COPY videos/ /app/videos/
COPY *.py /app/

CMD ["python", "app.py"]