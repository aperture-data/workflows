import os
import time
import argparse
import threading
import json

import numpy as np

import torch
import torch.distributed as dist
from torch.utils.data import DataLoader

from aperturedb import Utils
from aperturedb import PyTorchDataset
from aperturedb import CommonLibrary

from infer import BboxDetector as BboxDetector

resize_scale = 0.5
stop = False

def push_to_aperturedb(db, img_id, detections, classes, source, confidence_threshold):

    q = [{
        "FindImage": {
            "_ref": 1,
            "blobs": False,
            "constraints": {
                "_uniqueid": ["==", img_id]
            },
        }
    }, {
        "UpdateImage": {
            "ref": 1,
            "properties": {
                "wf_source": source
            }
        }
    }]

    for box, score, label in zip(detections["boxes"], detections["scores"], detections["labels"]):

        if float(score) < confidence_threshold:
            continue

        box = box.detach().cpu().numpy() # TODO: needs to be changed when using GPU
        idx = int(label)
        label = classes[idx]

        abb = {
            "AddBoundingBox": {
                "image_ref": 1,
                "label": label,
                "rectangle": {
                    "x": int(box[0] / resize_scale),
                    "y": int(box[1] / resize_scale),
                    "width":  int((box[2] - box[0]) / resize_scale),
                    "height": int((box[3] - box[1]) / resize_scale)
                },
                "properties": {
                    "wf_source": source,
                    "wf_confidence": float(score),
                },
            }
        }

        q.append(abb)

    db.query(q)

    if not db.last_query_ok():
        db.print_last_response()

def cleanup_bboxes_from_aperturedb(db, source):

    q = [{
        "DeleteBoundingBox": {
            "constraints": {
                "wf_source": ["==", source]
            }
        }
    }]

    print(f"Cleaning up bboxes with source: {source} from ApertureDB...")

    db.query(q)

    if not db.last_query_ok():
        db.print_last_response()

    print(f"Done cleaning up.")

def push_to_aperturedb_queue(db_obj, queue, classes, params):

    model_name = params.model_name

    # print("Starting thread", flush=True)

    global stop

    db = db_obj.create_new_connection()

    counter = 0
    while True:
        if stop and len(queue) == 0:
            # print("Thread push_to_aperturedb_queue stopped.")
            return

        if len(queue) == 0:
            time.sleep(1)
            continue

        img_id, detections = queue.pop(0)
        push_to_aperturedb(db, img_id, detections, classes, model_name, params.confidence_threshold)

        counter = 0

def main(params):

    print(f"Connecting to ApertureDB...")

    db = CommonLibrary.create_connector()

    dbutils = Utils.Utils(db)
    dbutils.create_entity_index("_BoundingBox", "wf_source")

    if params.clean:
        cleanup_bboxes_from_aperturedb(db, params.model_name)

    q = [{
        "FindImage": {
            "constraints": {
                "wf_source": ["!=", params.model_name],
            },
            "results": {
                "count": True
            }
        }
    }]

    res, _ = db.query(q)
    try:
        total = res[0]["FindImage"]["count"]
    except:
        print("Error getting total images:")
        db.print_last_response()
        total = 0

    if total == 0:
        print("No images to process.")
        return

    q = [{
        "FindImage": {
            "constraints": {
                "wf_source": ["!=", params.model_name],
            },
            "operations": [
                {
                    "type": "resize",
                    "scale": resize_scale
                }
            ],
            "results": {
                "list": ["_uniqueid"]
            }
        }
    }]

    if params.max_retrieved > 0:
        q[0]["FindImage"]["limit"] = params.max_retrieved

    print(f"Creating dataset...")
    try:
        dataset = PyTorchDataset.ApertureDBDataset(db, q, batch_size=1, label_prop="_uniqueid")
    except Exception as e:
        print("Error creating dataset:", e)
        dataset = []

    total = len(dataset)
    print("Total images in the dataset:", total)

    if total == 0:
        print("No images to process.")
        return

    batch_size = 1

    print(f"Loading model {params.model_name}...")
    detector = BboxDetector(model_name=params.model_name)

    # === Distributed Data Loader Sequential
    data_loader = DataLoader(
        dataset,
        batch_size=batch_size,          # pick random values here to test
        num_workers=1,          # num_workers > 1 to test multiprocessing works
        pin_memory=True,
        drop_last=True,
        prefetch_factor=4,
    )

    queue = [] # detection queue
    thds_push = []
    for i in range(4):
        # These threads will push detections as BoundingBoxes on ApertureDB
        x = threading.Thread(target=push_to_aperturedb_queue,
                            args=(db, queue, detector.classes, params))
        x.start()
        thds_push.append(x)

    start = time.time()
    imgs = 0

    print(f"Starting object detection on {len(data_loader)} images...", flush=True)
    for img, img_id in data_loader:
        start_infer = time.time()

        # https://numpy.org/devdocs/numpy_2_0_migration_guide.html#adapting-to-changes-in-the-copy-keyword
        detections = detector.infer(np.array(img[0].squeeze(), dtype=None, copy=None))

        # Add detections to the queue
        queue.append((img_id[0], detections))

        imgs += 1
        if imgs % 10 == 0:
            completion = int(imgs / total * 100)
            imgs_per_sec = imgs / (time.time() - start)
            print(f"\r  {completion}% completed @ {imgs_per_sec:.2f} imgs/s \t", end="", flush=True)

            # import gc
            # gc.collect()

        if params.max_retrieved > 0 and imgs > params.max_retrieved:
            break

    imgs_per_sec = imgs / (time.time() - start)
    print(f"\r 100% complete @ {imgs_per_sec:.2f} imgs/s \t", flush=True)
    print(f"Detection Took {time.time() - start:.2f}s", flush=True)

    start = time.time()
    print("Waiting for push to finish...", flush=True)

    global stop
    stop = True

    for x in thds_push:
        x.join()

    print(f"Waiting for push to finish took: {time.time() - start:.2f}s", flush=True)

def get_args():
    obj = argparse.ArgumentParser()

    # Run Config
    obj.add_argument('-dataset_mode', type=str, default="sequential")

    obj.add_argument('-model_name', type=str,
                     default=os.environ.get('MODEL_NAME', "frcnn-mobilenet"))

    obj.add_argument('-max_retrieved', type=int, default=0) # 0 means all

    obj.add_argument('-confidence_threshold', type=float,
                     default=os.environ.get('CONFIDENCE_THRESHOLD', '0.7'))

    obj.add_argument('-clean',    type=bool,
                     default=os.environ.get('CLEAN', 'false').lower() in ('true', '1', 't'))

    params = obj.parse_args()

    if params.model_name not in ["frcnn-mobilenet", "frcnn-resnet", "retinanet"]:
        raise ValueError("Invalid model name. Options: frcnn-mobilenet, frcnn-resnet, retinanet.")

    return params

if __name__ == "__main__":
    args = get_args()

    start = time.time()
    # Needed for init_process_group
    os.environ['MASTER_ADDR'] = 'localhost'
    os.environ['MASTER_PORT'] = '12355'

    dist.init_process_group("gloo", rank=0, world_size=1)

    try:
        main(args)
    except Exception as e:
        print(e)
        print("Something went wrong, exiting...")
    except KeyboardInterrupt:
        stop = True
        print("Keyboard interrupt, exiting...")

    dist.destroy_process_group()

    print("Done, bye.")
