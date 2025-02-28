#!/bin/bash
set -e

BATCH_SIZE=${BATCH_SIZE:=100}
export BATCH_SIZE
NUM_WORKERS=${NUM_WORKERS:=8}
export NUM_WORKERS
CLEAN=${CLEAN:=false}
export CLEAN
SAMPLE_COUNT=${SAMPLE_COUNT:=-1}
export SAMPLE_COUNT
DATASET=${DATASET:=coco}
export DATASET
INCLUDE_TRAIN=${INCLUDE_TRAIN:=false}
export INCLUDE_TRAIN

LOAD_CELEBAHQ=${LOAD_CELEBAHQ:false}
export LOAD_CELEBAHQ

if [ -z "${WF_DATA_SOURCE_GCP_BUCKET}" ]; then
    echo "Please set the WF_DATA_SOURCE_GCP_BUCKET environment variable"
    exit 1
fi

gcloud config set auth/disable_credentials True


build_coco() {
    APP="Dataset ingest (coco)"
    adb utils log --level INFO "${APP}: Start"

    date
    echo "Downloading val data..."
    bash download_coco.sh val

    if [[ $INCLUDE_TRAIN == true ]]; then
        echo "Downloading train data..."
        bash download_coco.sh train
    fi

    date
    adb utils log --level INFO "${APP}: Loading begins"
    echo "loading data..."
    python3 ingestion_demo_trial.py -R /app/input -C $CLEAN -B $BATCH_SIZE -W $NUM_WORKERS -S $SAMPLE_COUNT -T $INCLUDE_TRAIN

    # Validation
    python3 validate_db.py -input_file_path=/app/input/val -stages=val
    if [[ $INCLUDE_TRAIN == true ]]; then
        python3 validate_db.py -input_file_path=/app/input/train -stages=train
    fi

    date
    echo "All Done. Bye."
    adb utils log --level INFO "{$APP}: Successful completion"
}


build_faces() {
    APP="Dataset ingest (faces)"
    DIR="/app/input/faces"
    gcloud storage rsync gs://${WF_DATA_SOURCE_GCP_BUCKET}/workflows_streaming/faces ${DIR}
    cd ${DIR}


    # Ingest the CSV files
    adb utils log --level INFO "${APP}: Loading faces dataset"
    bash /app/build_faces/load.sh
    adb utils log --level INFO "${APP}: Successful completion"
}

case ${DATASET} in
    coco)
        cd /app/build_coco
        build_coco
        ;;
    faces)
        cd /app/build_faces
        build_faces
        ;;
    *)
        echo "${APP}: Unknown dataset ${DATASET}"
        exit 1
        ;;
esac
