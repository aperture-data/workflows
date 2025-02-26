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

if [ -z "${WF_DATA_SOURCE_AWS_BUCKET}" ]; then
    echo "Please set the WF_DATA_SOURCE_AWS_BUCKET environment variable"
    exit 1
fi
if [ -z "${WF_DATA_SOURCE_AWS_CREDENTIALS}" ]; then
    echo "Please set the WF_DATA_SOURCE_AWS_CREDENTIALS environment variable"
    exit 1
fi

AWS_ACCESS_KEY_ID=$(jq -r .access_key <<< ${WF_DATA_SOURCE_AWS_CREDENTIALS})
export AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=$(jq -r .secret_key <<< ${WF_DATA_SOURCE_AWS_CREDENTIALS})
export AWS_SECRET_ACCESS_KEY




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
    aws s3 sync --quiet s3://${WF_DATA_SOURCE_AWS_BUCKET}/processed_faces ${DIR}
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
    # Ingest the CSV files
    adb utils log --level INFO "${APP}: Loading faces dataset"
    bash load.sh
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
