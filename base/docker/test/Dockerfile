# Test image for base workflow modules
ARG VERSION=latest
FROM aperturedata/workflows-base:${VERSION}

# Install test dependencies
RUN pip install --no-cache-dir pytest pytest-timeout

# Copy test files
COPY *.py /app

# Set working directory
WORKDIR /app

# Run tests with pytest
CMD ["pytest", "-v", "/app/"]

