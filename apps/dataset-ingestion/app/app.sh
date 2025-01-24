#!/bin/bash
set -e

BATCH_SIZE=${BATCH_SIZE:=-1}
export BATCH_SIZE
NUM_WORKERS=${NUM_WORKERS:=8}
export NUM_WORKERS
CLEAN=${CLEAN:=true}
export CLEAN
SAMPLE_COUNT=${SAMPLE_COUNT:=10}
export SAMPLE_COUNT
DATASET=${DATASET:=coco}
export DATASET

APP="Dataset ingest"

err_handler() {
    adb utils log --level ERROR "${APP}: Error in line $1, code $2"
}

build_coco() {
    adb utils log --level INFO "${APP} (coco): Start"
    trap 'err_handler $LINENO ${?}' ERR

    date
    echo "Downloading data..."
    bash download_coco.sh

    date
    echo "Generating input files for data loaders..."
    python3 generate_coco_csv.py -input_file_path=/app/input -output_file_path=/app/input -generate_embeddings=False

    adb utils log --level INFO "${APP} (coco): Loading begins"

    date
    echo "loading data..."
    python3 ingestion_demo_trial.py /app/input

    python3 validate_db.py -input_file_path=/app/input

    date
    echo "All Done. Bye."

    adb utils log --level INFO "{$APP} (coco): Successful completion"

    python3 ingest_dataset.py
}
case ${DATASET} in
    coco)
        cd /app/build_coco
        build_coco
        ;;
    *)
        adb utils log --level ERROR "${APP}: Unknown dataset ${DATASET}"
        exit 1
        ;;
esac
