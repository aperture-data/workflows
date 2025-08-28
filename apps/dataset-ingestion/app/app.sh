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

STATUS_SCRIPT="/app/status_tools.py"

if [ -z "${WF_DATA_SOURCE_GCP_BUCKET}" ]; then
    echo "Please set the WF_DATA_SOURCE_GCP_BUCKET environment variable"
    python3 $STATUS_SCRIPT --completed 0 \
      --error-message "WF_DATA_SOURCE_GCP_BUCKET is not set" \
      --error-code "workflow_error" --status "failed"
    exit 1
fi

# gcloud uses it's bundled python. Do not inject our sitecustomize.
PYTHONPATH='' gcloud config set auth/disable_credentials True

build_coco() {
    APP="Dataset ingest (coco)"
    python3 $STATUS_SCRIPT --completed 0 \
      --phases downloading \
      --phases ingesting_images \
      --phases ingesting_bounding_boxes \
      --phases ingesting_polygons \
      --phases ingesting_images \
      --phases ingesting_connections \
      --phases ingesting_descriptors \
      --phases ingesting_connections  \
      --phase downloading

    adb utils log --level INFO "${APP}: Start"

    date
    echo "Downloading val data..."

    python3 $STATUS_SCRIPT --completed 0
    bash download_coco.sh val
    python3 $STATUS_SCRIPT --completed 100

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
    python3 $STATUS_SCRIPT --completed 0 \
      --phases downloading \
      --phases ingesting_images \
      --phases ingesting_descriptors \
      --phases ingesting_connections \
      --phases ingesting_descriptors \
      --phases ingesting_connections \
      --phases ingesting_images \
      --phases ingesting_polygons \
      --phases ingesting_bounding_boxes \
      --phases ingesting_descriptors \
      --phases ingesting_connections \
      --phase downloading

    gcloud storage rsync --recursive gs://${WF_DATA_SOURCE_GCP_BUCKET}/workflows/faces ${DIR}
    cd ${DIR}

    mkdir -p images
    cd images

    # Extract all the images, first aligned, then unaligned
    echo "Extracting images"
    unzip -n -q ../img_align_celeba.zip
    cd ../

    echo "Extracting HQ images"
    mkdir -p celeba-hq
    cd celeba-hq
    unzip -n -q ../CelebAMask-HQ.zip
    cd -

    adb utils log --level INFO "${APP} faces: Generating CelebA dataset"

    #Extract the embeddings
    echo "Extracting embeddings"
    tar xf celebA.csv_clip_pytorch_embeddings.tgz
    tar xf celebA.csv_facenet_pytorch_embeddings.tgz
    tar xf hqimages.adb.csv_facenet_pytorch_embeddings.tgz


    cd /app/build_faces
    python3 $STATUS_SCRIPT --completed 100
    # Ingest the CSV files
    adb utils log --level INFO "${APP}: Loading faces dataset"
    python3 ../log_processor.py 'bash load.sh'
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
