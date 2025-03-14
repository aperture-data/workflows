#!/bin/bash
set -e

BATCH_SIZE=${BATCH_SIZE:=1}
NUM_WORKERS=${NUM_WORKERS:=8}
CLEAN=${CLEAN:=true}
SAMPLE_COUNT=${SAMPLE_COUNT:=10}


cd /app/build_faces

python3 create_descriptorsets.py

echo "Ingesting"
cd /app/input/faces
adb ingest from-csv celebA.csv_clip_pytorch_embeddings_metadata.adb.csv --ingest-type DESCRIPTOR --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}
adb ingest from-csv celebA.csv_clip_pytorch_embeddings_connection.adb.csv --ingest-type CONNECTION --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}

adb ingest from-csv celebA.csv_facenet_pytorch_embeddings_metadata.adb.csv --ingest-type DESCRIPTOR --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}
adb ingest from-csv celebA.csv_facenet_pytorch_embeddings_connection.adb.csv --ingest-type CONNECTION --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}

adb ingest from-csv hqpolygons.adb.csv --ingest-type POLYGON --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}
adb ingest from-csv hqbboxes.adb.csv --ingest-type BOUNDING_BOX --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}

adb ingest from-csv hqimages.adb.csv_facenet_pytorch_embeddings_metadata.adb.csv --ingest-type DESCRIPTOR --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}
adb ingest from-csv hqimages.adb.csv_facenet_pytorch_embeddings_connection.adb.csv --ingest-type CONNECTION --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}

cd /app/build_faces
python3 validate_db.py
echo "All Done. Bye."
