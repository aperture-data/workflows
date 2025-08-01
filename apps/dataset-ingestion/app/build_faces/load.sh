#!/bin/bash
set -e

BATCH_SIZE=${BATCH_SIZE:=1}
NUM_WORKERS=${NUM_WORKERS:=8}
CLEAN=${CLEAN:=true}
SAMPLE_COUNT=${SAMPLE_COUNT:=10}

STATUS_SCRIPT="/app/status.py"
if [[ ${CLEAN} == "true" ]]; then
    echo "Cleaning the database"
    adb utils execute remove_all --force
fi
cd /app/build_faces

python3 create_indexes.py
python3 create_descriptorsets.py

echo "Ingesting"
cd /app/input/faces
python3 $STATUS_SCRIPT --completed 0 --phase ingesting_images
adb ingest from-csv pruned_celebA.csv --transformer image_properties --transformer common_properties  --ingest-type IMAGE --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS}  --sample-count ${SAMPLE_COUNT}

python3 $STATUS_SCRIPT --completed 0 --phase ingesting_descriptors
adb ingest from-csv celebA.csv_clip_pytorch_embeddings_metadata.adb.csv --ingest-type DESCRIPTOR --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}

python3 $STATUS_SCRIPT --completed 0 --phase ingesting_connections
adb ingest from-csv celebA.csv_clip_pytorch_embeddings_connection.adb.csv --ingest-type CONNECTION --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}

python3 $STATUS_SCRIPT --completed 0 --phase ingesting_descriptors
adb ingest from-csv celebA.csv_facenet_pytorch_embeddings_metadata.adb.csv --ingest-type DESCRIPTOR --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}

python3 $STATUS_SCRIPT --completed 0 --phase ingesting_connections
adb ingest from-csv celebA.csv_facenet_pytorch_embeddings_connection.adb.csv --ingest-type CONNECTION --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}

python3 $STATUS_SCRIPT --completed 0 --phase ingesting_images
adb ingest from-csv hqimages.adb.csv --ingest-type IMAGE --transformer common_properties --transformer image_properties --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}

python3 $STATUS_SCRIPT --completed 0 --phase ingesting_polygons
adb ingest from-csv hqpolygons.adb.csv --ingest-type POLYGON --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}

python3 $STATUS_SCRIPT --completed 0 --phase ingesting_bounding_boxes
adb ingest from-csv hqbboxes.adb.csv --ingest-type BOUNDING_BOX --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}

python3 $STATUS_SCRIPT --completed 0 --phase ingesting_descriptors
adb ingest from-csv hqimages.adb.csv_facenet_pytorch_embeddings_metadata.adb.csv --ingest-type DESCRIPTOR --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}

python3 $STATUS_SCRIPT --completed 0 --phase ingesting_connections
adb ingest from-csv hqimages.adb.csv_facenet_pytorch_embeddings_connection.adb.csv --ingest-type CONNECTION --batchsize ${BATCH_SIZE} --num-workers ${NUM_WORKERS} --sample-count ${SAMPLE_COUNT}

cd /app/build_faces
python3 validate_db.py
echo "All Done. Bye."
