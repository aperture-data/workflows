import os
import sys
from aperturedb.Utils import Utils
from aperturedb.CommonLibrary import create_connector

def ingest_coco(cli_args):
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

    args = {
        "images": "IMAGE",
        "bboxes": "BOUNDING_BOX",
        "pixelmaps": "IMAGE",
        "img_pixelmap_connections": "CONNECTION",
        "polygons": "POLYGON",
        "images.adb.csv_clip_pytorch_embeddings_metadata": "DESCRIPTOR",
        "images.adb.csv_clip_pytorch_embeddings_connection": "CONNECTION"
    }
    stages = ["val"]
    if cli_args.train == "true":
        stages.append("train")

    objs = ["images",
            "bboxes",
            "polygons",
            "pixelmaps",
            "img_pixelmap_connections",
            "images.adb.csv_clip_pytorch_embeddings_metadata",
            "images.adb.csv_clip_pytorch_embeddings_connection"]


    set_name = "ViT-B/16"
    dbutils.add_descriptorset(set_name, 512,
                              metric=["CS"],
                              engine=["HNSW"])

    for stage in stages:
        for obj in objs:
            common_command = f"adb ingest from-csv {cli_args.root_dir}/{stage}/{stage}_{obj}.adb.csv --ingest-type {args[obj]} --batchsize {cli_args.batch_size} --num-workers {cli_args.num_workers} --no-use-dask --sample-count {cli_args.sample_count} --stats"
            transformers = "--transformer common_properties --transformer image_properties" if obj == "images" else ""
            command = f"{common_command} {transformers}"
            print(command, flush=True)
            os.system(command)

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

def main(args):
    if args.clean == "true":
        print("Cleaning DB")
        clean_db()
    ingest_coco(args)
    update_adb_source()

import argparse

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-B", "--batch_size", type=int,
                        default=100, help="Batch size")
    parser.add_argument("-W", "--num_workers", type=int,
                        default=32, help="Number of workers")
    parser.add_argument("-S", "--sample_count", type=int,
                        default=10, help="Sample count")
    parser.add_argument("-C", "--clean", type=str,
                        default="true", help="Clean DB")
    parser.add_argument("-R", "--root_dir", type=str,
                        default="data", help="Root directory")
    parser.add_argument("-T", "--train", type=str, choices=["true", "false"],
                        default="false", help="include train dataset")
    return parser.parse_args()

if __name__ in "__main__":
    args = parse_args()
    print(args)
    main(args)