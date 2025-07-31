#!/bin/bash

set -e

HOSTNAME=$(hostname -f) python3 app.py &
PID=$!

# Wait for the knn-extensions app to start
while [ -z "$(lsof -i:3000)" ]; do
    echo "Waiting for knn-extensions app to start on port 3000..."
    sleep 1
done
echo "knn-extensions app is up."

wait $PID
STATUS=$?

exit $STATUS
