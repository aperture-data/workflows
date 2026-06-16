#!/bin/bash
set -e

mkdir -p input

# Download required datasets
./download_data.sh

if [[ "$MINIMAL" == true ]]; then

    export ENGINES=adb # ApertureDB only

    echo "Building aperturedb sets..."
    python3 ingest.py > output/ingest.log

    echo "Running knn_samples=10..."
    python3 knn.py -knn_samples=10 -sizes=1000 \
                   -keep_prev_output=false > output/knn_samples_10.log 2>&1

    echo "Plotting..."
    python3 plot.py > output/plot.log 2>&1

    echo "Done. Bye!"
    exit 0
fi

# if BUILD_SETS is true, then build the sets and exit
if [[ "$BUILD_SETS" == true ]]; then

    echo "Building sets..."
    python3 ingest.py > output/ingest.log
    echo "Done sets."
    exit 0
fi

echo "Verifying sets..."
python3 verify.py > output/verify_sets.log

# if verification fails, exit 1
if [[ $? -ne 0 ]]; then
    echo "ERROR: Verification failed."
    exit 1
fi
echo "Verification succeeded."

echo "Running knn_samples=10..."
python3 knn.py -knn_samples=10  -keep_prev_output=false > output/knn_samples_10.log 2>&1

echo "Running knn_samples=100..."
python3 knn.py -knn_samples=100 -keep_prev_output=true > output/knn_samples_100.log 2>&1

echo "Plotting..."
python3 plot.py > output/plot.log 2>&1

echo "Done. Bye!"
