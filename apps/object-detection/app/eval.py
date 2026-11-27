from aperturedb.Constraints import predicate
import os
import time
import argparse
import threading
import json
import queue

import numpy as np

import torch
import torch.distributed as dist
from torch.utils.data import DataLoader

from aperturedb import Utils
from aperturedb import PyTorchDataset
from aperturedb import CommonLibrary

from aperturedb_workflows.connection_pool import ConnectionPool

from infer import BboxDetector as BboxDetector

resize_scale = 0.5


def push_to_aperturedb(pool, img_id, detections, classes, source, confidence_threshold):

    q = [{'FindImage': {'_ref': 1, 'blobs': False, 'constraints': predicate('_uniqueid', '==', img_id)}}, {
        'UpdateImage': {'ref': 1, 'properties': {'wf_od_model': source}}}]

    for box, score, label in zip(detections["boxes"], detections["scores"], detections["labels"]):

        if float(score) < confidence_threshold:
            continue

        box = box.detach().cpu().numpy()  # TODO: needs to be changed when using GPU
        idx = int(label)
        label = classes[idx]

        abb = {
            'AddBoundingBox': {
                'class': 'BoundingBoxes',
                'image_ref': 1,
                'label': label,
                'rectangle': {
                    'x': int(
                        box[0] / resize_scale),
                    'y': int(
                        box[1] / resize_scale),
                    'width': int(
                        (box[2] - box[0]) / resize_scale),
                    'height': int(
                        (box[3] - box[1]) / resize_scale)},
                'properties': {
                    'wf_od_model': source,
                    'wf_od_confidence': float(score)}}}

        q.append(abb)

    status, _, _ = pool.execute_query(q)
    if status != 0:
        raise RuntimeError("Failed to save object detections to ApertureDB")


def cleanup_bboxes_from_aperturedb(pool, source):

    q = [{'DeleteBoundingBox': {'constraints': predicate('wf_od_model', '!=', None)}}, {'UpdateImage': {
        'constraints': predicate('wf_od_model', '!=', None), 'remove_props': ['wf_od_model']}}]

    print(f"Cleaning up bboxes with source: {source} from ApertureDB...")

    with pool.get_connection() as db:
        db.query(q)

        if not db.last_query_ok():
            db.print_last_response()

    print(f"Done cleaning up.")


def push_to_aperturedb_queue(pool, pending, errors, classes, params):

    model_name = params.model_name

    while True:
        item = pending.get()
        try:
            if item is None:
                return
            img_id, detections = item
            try:
                push_to_aperturedb(pool, img_id, detections, classes,
                                   model_name, params.confidence_threshold)
            except Exception as error:
                errors.put(error)
        finally:
            pending.task_done()


def raise_push_error(errors):
    try:
        error = errors.get_nowait()
    except queue.Empty:
        return
    raise RuntimeError("Failed to push object detections") from error


def main(params):

    print(f"Connecting to ApertureDB...")

    pool = ConnectionPool()

    with pool.get_utils() as dbutils:
        dbutils.create_entity_index("bounding_box:BoundingBoxes", "wf_od_model")

    if params.clean:
        cleanup_bboxes_from_aperturedb(pool, params.model_name)

    # Native datasets page with offsets. Freeze the candidate IDs before push
    # workers update wf_od_model, so those writes cannot shrink the selection.
    q = [{'FindImage': {'constraints': predicate(
        'wf_od_model', '!=', params.model_name), 'blobs': False,
        'results': {'list': ['_uniqueid']}, 'uniqueids': True}}]
    if params.max_retrieved > 0:
        q[0]['FindImage']['limit'] = params.max_retrieved

    with pool.get_connection() as db:
        res, _ = db.query(q)
        try:
            response = res[0]["FindImage"]
            if response.get('status', 0) != 0:
                raise RuntimeError("Failed to select images for object detection")
            image_ids = [image['_uniqueid']
                         for image in response.get('entities', [])]
        except BaseException:
            print("Error getting total images:")
            db.print_last_response()
            raise

    if not image_ids:
        print("No images to process.")
        return

    q = [{'FindImage': {'constraints': predicate('_uniqueid', 'in', image_ids), 'operations': [
        {'type': 'resize', 'scale': resize_scale}], 'results': {'list': ['_uniqueid']}, 'uniqueids': True}}]

    if params.max_retrieved > 0:
        q[0]["FindImage"]["limit"] = params.max_retrieved

    print(f"Creating dataset...")
    try:
        with pool.get_connection() as client:
            dataset = PyTorchDataset.ApertureDBDataset(
                client, q, batch_size=1, label_prop="_uniqueid")
    except Exception as e:
        print("Error creating dataset:", e)
        raise

    total = len(dataset)
    print("Total images in the dataset:", total)

    if total == 0:
        print("No images to process.")
        return

    batch_size = 1

    print(f"Loading model {params.model_name}...")
    detector = BboxDetector(model_name=params.model_name,
                            confidence=params.confidence_threshold)

    # === Distributed Data Loader Sequential
    data_loader = DataLoader(
        dataset,
        batch_size=batch_size,          # pick random values here to test
        num_workers=1,          # num_workers > 1 to test multiprocessing works
        pin_memory=True,
        drop_last=True,
        prefetch_factor=4,
    )

    pending = queue.Queue()
    errors = queue.Queue()
    thds_push = []
    start = time.time()
    imgs = 0

    try:
        for i in range(4):
            # These threads push detections as BoundingBoxes on ApertureDB.
            worker = threading.Thread(target=push_to_aperturedb_queue,
                                      args=(pool, pending, errors, detector.classes, params))
            worker.start()
            thds_push.append(worker)

        print(
            f"Starting object detection on {len(data_loader)} images...", flush=True)
        for img, img_id in data_loader:
            raise_push_error(errors)

            # https://numpy.org/devdocs/numpy_2_0_migration_guide.html#adapting-to-changes-in-the-copy-keyword
            detections = detector.infer(
                np.array(img[0].squeeze(), dtype=None, copy=None))

            pending.put((img_id[0], detections))

            imgs += 1
            if imgs % 10 == 0:
                completion = int(imgs / total * 100)
                imgs_per_sec = imgs / (time.time() - start)
                print(
                    f"\r  {completion}% completed @ {imgs_per_sec:.2f} imgs/s \t", end="", flush=True)

                import gc
                gc.collect()

            if params.max_retrieved > 0 and imgs > params.max_retrieved:
                break

        inference_seconds = time.time() - start
    finally:
        waiting = time.time()
        print("Waiting for push to finish...", flush=True)
        for _ in thds_push:
            pending.put(None)
        for worker in thds_push:
            worker.join()
        print(
            f"Waiting for push to finish took: {time.time() - waiting:.2f}s", flush=True)

    raise_push_error(errors)
    imgs_per_sec = imgs / inference_seconds
    print(f"\r 100% complete @ {imgs_per_sec:.2f} imgs/s \t", flush=True)
    print(f"Detection Took {inference_seconds:.2f}s", flush=True)


def get_args():
    obj = argparse.ArgumentParser()

    # Run Config
    obj.add_argument('-model_name', type=str,
                     default=os.environ.get('MODEL_NAME', "frcnn-mobilenet"))

    # Processing more than 10000 images will take a long time,
    # by which the list of images may even change.
    # So, by default, we don't process more than 10000 images on a single run.
    # This also helps preventing the workflow execution from running out of memory.
    obj.add_argument('-max_retrieved', type=int,
                     default=os.environ.get('MAX_RETRIEVED', 10000))  # 0 means all

    obj.add_argument('-confidence_threshold', type=float,
                     default=os.environ.get('CONFIDENCE_THRESHOLD', '0.7'))

    obj.add_argument('-clean',    type=bool,
                     default=os.environ.get('CLEAN', 'false').lower() in ('true', '1', 't'))

    params = obj.parse_args()

    if params.model_name not in ["frcnn-mobilenet", "frcnn-resnet", "retinanet"]:
        raise ValueError(
            "Invalid model name. Options: frcnn-mobilenet, frcnn-resnet, retinanet.")

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
        raise
    except KeyboardInterrupt:
        print("Keyboard interrupt, exiting...")
    finally:
        dist.destroy_process_group()

    print("Done, bye.")
