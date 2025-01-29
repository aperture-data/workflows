#!/bin/bash

BATCH_SIZE=${BATCH_SIZE:=1}
NUM_WORKERS=${NUM_WORKERS:=8}
CLEAN=${CLEAN:=true}
SAMPLE_COUNT=${SAMPLE_COUNT:=10}

if [[ ${CLEAN} == "true" ]]; then
    echo "Cleaning the database"
    adb utils execute remove_all --force
fi

python3 create_indexes.py
python3 create_descriptorsets.py

echo "Removing duplicates"
python3 remove_duplicates.py celebA.csv
mv pruned_celebA.csv celebA.csv

echo "Ingesting"

adb ingest from-csv celebA.csv --transformer image_properties --transformer common_properties  --ingest-type IMAGE --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS}  --sample-count ${SAMPLE_COUNT}
adb ingest from-csv celebA.csv_clip_pytorch_embeddings_metadata.adb.csv --ingest-type DESCRIPTOR --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}
adb ingest from-csv celebA.csv_clip_pytorch_embeddings_connection.adb.csv --ingest-type CONNECTION --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}

adb ingest from-csv celebA.csv_facenet_pytorch_embeddings_metadata.adb.csv --ingest-type DESCRIPTOR --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}
adb ingest from-csv celebA.csv_facenet_pytorch_embeddings_connection.adb.csv --ingest-type CONNECTION --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}

cd celeba-hq
adb ingest from-csv hqimages.adb.csv --ingest-type IMAGE --transformer common_properties --transformer image_properties --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}
adb ingest from-csv hqpolygons.adb.csv --ingest-type POLYGON --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}
adb ingest from-csv hqbboxes.adb.csv --ingest-type BOUNDING_BOX --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}
cd ..

adb ingest from-csv hqimages.adb.csv_facenet_pytorch_embeddings_metadata.adb.csv --ingest-type DESCRIPTOR --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}
adb ingest from-csv hqimages.adb.csv_facenet_pytorch_embeddings_connection.adb.csv --ingest-type CONNECTION --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}

python3 validate_db.py
echo "All Done. Bye."