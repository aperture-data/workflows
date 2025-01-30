import os
import sys
from aperturedb.Utils import Utils
from aperturedb.CommonLibrary import create_connector

def ingest_coco():
    """
    Entire ingestion pipeline for the COCO dataset + some videos + trial demo related data.
    """
    print("Creating entity indexes")
    dbutils = Utils(create_connector())
    dbutils.create_entity_index("_Image", "id")
    dbutils.create_entity_index("_Image", "yfcc_id")
    dbutils.create_entity_index("_Image", "seg_id")
    dbutils.create_entity_index("_BoundingBox", "bbox_id")
    dbutils.create_entity_index("_Polygon", "ann_id")
    dbutils.create_entity_index("_Descriptor", "yfcc_id")

    root_dir = sys.argv[1] if len(sys.argv) > 1 else "data"
    sample_count = os.environ.get("SAMPLE_COUNT", 10)
    num_workers = os.environ.get("NUM_WORKERS", 32)
    batch_size = os.environ.get("BATCH_SIZE", 100)
    args = {
        "images": "IMAGE",
        "bboxes": "BOUNDING_BOX",
        "pixelmaps": "IMAGE",
        "img_pixelmap_connections": "CONNECTION",
        "polygons": "POLYGON",
        "images.adb.csv_clip_pytorch_embeddings_metadata": "DESCRIPTOR",
        "images.adb.csv_clip_pytorch_embeddings_connection": "CONNECTION"
    }
    stages = ["val", "train"]
    objs = ["images",
            "bboxes",
            "polygons",
            "pixelmaps",
            "img_pixelmap_connections",
            "images.adb.csv_clip_pytorch_embeddings_metadata",
            "images.adb.csv_clip_pytorch_embeddings_connection"]


    set_name = "ViT-B/16"
    dbutils.add_descriptorset(set_name, 512,
                              metric=["L2"],
                              engine=["FaissHNSWFlat"])

    for stage in stages:
        for obj in objs:
            common_command = f"adb ingest from-csv {root_dir}/{stage}_{obj}.adb.csv --ingest-type {args[obj]} --batchsize {batch_size} --num-workers {num_workers} --no-use-dask --sample-count {sample_count} --stats"
            transformers = "--transformer common_properties --transformer image_properties" if obj == "images" else ""
            commmd = f"{common_command} {transformers}"
            print(commmd, flush=True)
            os.system(commmd)

def update_adb_source():
    command = f"adb transact from-json-file update_image_adb_source.json"
    print(command)
    os.system(command)

def add_ro_user():
    command = f"adb transact from-json-file create_ro_user_role.json"
    print(command)
    os.system(command)

def clean_db():
    utils: Utils = Utils(create_connector())
    assert utils.remove_all_objects() == True

def main():
    if os.environ.get("CLEAN", "true") == "true":
        print("Cleaning DB")
        clean_db()
    ingest_coco()
    update_adb_source()


if __name__ in "__main__":
    main()