FROM python:3.10-slim
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy test files and data
COPY images/ /app/images/
COPY pdfs/ /app/pdfs/
COPY videos/ /app/videos/
COPY *.py /app/

CMD ["python", "app.py"]