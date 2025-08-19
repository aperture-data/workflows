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
COPY *.py /app/
COPY photos_with_text/ /app/photos_with_text/
COPY pdfs/ /app/pdfs/

CMD ["python", "app.py"]