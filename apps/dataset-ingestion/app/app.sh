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

LOAD_CELEBAHQ=${LOAD_CELEBAHQ:false}
export LOAD_CELEBAHQ

APP="Dataset ingest"


build_coco() {
    adb utils log --level INFO "${APP} (coco): Start"

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
}

build_faces() {
    mkdir -p input
    mkdir -p output
    aws s3 sync --quiet s3://aperturedb-demos/faces input/
    cd input

    mkdir -p images
    cd images

    # Extract all the images, first aligned, then unaligned
    unzip -n -q ../img_align_celeba.zip
    cd ../../

    mkdir -p input/celeba-hq
    cd input/celeba-hq
    unzip -n -q ../CelebAMask-HQ.zip
    cd -

    adb utils log --level INFO "${APP} faces: Generating CelebA dataset"
    # Generate the CSV files
    python3 CelebA.py
    tar xf input/celebA.csv_clip_pytorch_embeddings.tgz
    tar xf input/celebA.csv_facenet_pytorch_embeddings.tgz
    tar xf input/hqimages.adb.csv_facenet_pytorch_embeddings.tgz


    echo "Removing duplicates"
    python3 remove_duplicates.py celebA.csv
    mv pruned_celebA.csv celebA.csv

    # to inspect the CSV files after container exits
    cp *.csv output/ -v

    if [[ $LOAD_CELEBAHQ == true ]]; then
        adb utils log --level INFO "${APP} faces: Generating CelebA-HQ dataset"
        cd celeba-hq
        bash setup.sh
        cp *.csv ../output/ -v
        cd ..
    fi

    # Ingest the CSV files
    adb utils log --level INFO "${APP} faces: Loading faces dataset"
    bash load.sh
    adb utils log --level INFO "${APP} faces: Successful completion"
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
