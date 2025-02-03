import argparse
import json
import os

from aperturedb import Utils
from aperturedb import ConnectionDataCSV
from aperturedb import ImageDataCSV
from aperturedb import BBoxDataCSV
from aperturedb import PolygonDataCSV
from aperturedb import DescriptorDataCSV
from aperturedb.CommonLibrary import create_connector, execute_query

def parse_count_result(r):
    if len(r) >= 1:
        cmd, res = list(r[0].items())[0]
        if res["status"] == 0 and "count" in res:
            return res["count"]
    return json.dumps(r)

def check_counts(json_result, csv_data, what):
    num_found = parse_count_result(json_result)
    num_expected = len(csv_data)
    if num_found == num_expected:
        print("All {} {} found".format(num_found, what))
        return True
    print("{} {} missing (found {} of {})".format(num_expected - num_found, what, num_found, num_expected))
    return False

def count_images(client, csv_folder, corpus):
    q = [ { "FindImage": {
            "constraints": {
                "id": [">", 0],
                "corpus": ["==", corpus]
            },
            "results": { "count": True },
            "blobs": False
        }}]

    _, r, _ = execute_query(client, q, [])

    in_csv_file = csv_folder + corpus + "_images.adb.csv"
    d = ImageDataCSV.ImageDataCSV(in_csv_file, check_image=False)
    return check_counts(r, d, "{} Images".format(corpus))

def count_bboxes(client, csv_folder, corpus):
    q = [ { "FindBoundingBox": {
            "constraints": {
                "bbox_id": [">", 0],
                "corpus": ["==", corpus]
            },
            "results": { "count": True }
        }}]

    _, r, _ = execute_query(client, q, [])

    in_csv_file = csv_folder + corpus + "_bboxes.adb.csv"
    d = BBoxDataCSV.BBoxDataCSV(in_csv_file)
    return check_counts(r, d, "{} BoundingBoxes".format(corpus))

def count_polygons(client, csv_folder, corpus):
    q = [ { "FindPolygon": {
            "constraints": {
                "corpus": ["==", corpus],
            },
            "results": { "count": True }
        }}]

    _, r, _ = execute_query(client, q, [])

    in_csv_file = csv_folder + corpus + "_polygons.adb.csv"
    d = PolygonDataCSV.PolygonDataCSV(in_csv_file)
    return check_counts(r, d, "{} Polygons".format(corpus))

def count_segmentations(client, csv_folder, corpus):
    q = [ { "FindImage": {
            "constraints": {
                "seg_id": [">", 0],
                "corpus": ["==", corpus]
            },
            "results": { "count": True },
            "blobs": False
        }}]

    _, r, _ = execute_query(client, q, [])

    in_csv_file = csv_folder + corpus + "_pixelmaps.adb.csv"
    d = ImageDataCSV.ImageDataCSV(in_csv_file, check_image=False)
    return check_counts(r, d, "{} Segmentations".format(corpus))

def count_segmentation_connections(client, csv_folder, corpus):
    q = [ { "FindConnection": {
            "with_class": "segmentation",
            "constraints": {
                "corpus": ["==", corpus]
            },
            "results": { "count": True }
        }}]

    _, r, _ = execute_query(client, q, [])

    in_csv_file = csv_folder + corpus + "_img_pixelmap_connections.adb.csv"
    d = ConnectionDataCSV.ConnectionDataCSV(in_csv_file)
    return check_counts(r, d, "{} Segmentation Connections".format(corpus))

def count_descriptors(client, csv_folder, corpus):
    """
    Checks there is exactly one descriptor for each image in DB,
    under the specific set name.
    """
    q = [ { "FindImage": {
            "_ref": 1,
            "constraints": {
                "id": [">", 0],
                "corpus": ["==", corpus]
            },
            "results": { "list": ["_uniqueid"] },
            "blobs": False
        }},
        { "FindDescriptor": {
            "set": "ViT-B/16",
            "is_connected_to":{
                "ref": 1
            },
            "results": {
                "list": ["_uniqueid"],
                "group_by_source": True
            }
        }}
        ]

    _, r, _ = execute_query(client, q, [])
    d_entities = r[1]["FindDescriptor"]["entities"]
    count = 0
    for img in r[0]["FindImage"]["entities"]:
        if img["_uniqueid"] not in d_entities:
            print(f"Descriptor not found for image {img['_uniqueid']}")
            return False
        count += 1
    in_csv_file = csv_folder + corpus + "_images.adb.csv_clip_pytorch_embeddings_metadata.adb.csv"
    d = DescriptorDataCSV.DescriptorDataCSV(in_csv_file)
    print(f"{count} descriptors found for {corpus} Images, input had {len(d)} descriptors")
    return True

def validate_loaded_coco_objects(client, csv_folder, corpora):

    print("Validating DB object counts against CSV data in {}...".format(csv_folder))

    found_everything = True
    for corpus in corpora:
        found_everything &= count_images(client, csv_folder, corpus)
        found_everything &= count_bboxes(client, csv_folder, corpus)
        found_everything &= count_polygons(client, csv_folder, corpus)
        found_everything &= count_segmentations(client, csv_folder, corpus)
        found_everything &= count_segmentation_connections(client, csv_folder, corpus)
        found_everything &= count_descriptors(client, csv_folder, corpus)

    return found_everything

def main(params):
    client = create_connector()
    utils = Utils.Utils(client)
    print(utils)


    if not validate_loaded_coco_objects(client, params.input_file_path + "/", params.stages.split(',')):
        print("Validation failed")
        exit(1)
    print("DB is complete :)")

def get_args():
    obj = argparse.ArgumentParser()

    # Input CSVs
    obj.add_argument('-input_file_path', type=str, default="data/")
    obj.add_argument('-stages', type=str, default=os.environ.get('STAGES', "val,train"))

    params = obj.parse_args()

    return params

if __name__ == "__main__":
    args = get_args()
    main(args)
