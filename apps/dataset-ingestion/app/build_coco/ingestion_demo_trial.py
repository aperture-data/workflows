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

def ingest_videos():
    video_data_path = "../video_sample_data"
    input_file = os.path.join(video_data_path, "yfcc100m_dataset_100_videos_urls.txt")
    output_video_path = os.path.join(video_data_path, "videos")
    csv_file = os.path.join(video_data_path, "videos", "videos.adb.csv")
    os.makedirs(output_video_path, exist_ok=True)
    videos.generate_video_csv(input_file, output_video_path)
    videos.download_videos(csv_file, "results/err_download.txt")

    db = create_connector()
    dbutils = Utils(db)
    dbutils.create_entity_index("_Video", "guid")
    dbutils.create_entity_index("Camera", "id")
    videos.load_all(db, video_data_path, 1, 1, "results/err_format.txt")

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