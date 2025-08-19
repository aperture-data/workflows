#!/bin/bash

set -e

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$SCRIPT_DIR/test"

echo "=== Embeddings Extraction Test Suite ==="
echo "Test directory: $TEST_DIR"

# Function to cleanup on exit
cleanup() {
    echo "Cleaning up..."
    cd "$TEST_DIR"
    docker-compose down --volumes --remove-orphans
}

# Set up cleanup trap
trap cleanup EXIT

# Build the test image
echo "Building test image..."
cd "$TEST_DIR"
docker-compose build test-base

# Run the tests
echo "Running tests..."
docker-compose up --abort-on-container-exit

# Check if tests passed
TEST_EXIT_CODE=$(docker-compose ps -q tests | xargs docker inspect -f '{{.State.ExitCode}}')

if [ "$TEST_EXIT_CODE" = "0" ]; then
    echo "✅ All tests passed!"
    exit 0
else
    echo "❌ Tests failed with exit code: $TEST_EXIT_CODE"
    exit 1
fi
