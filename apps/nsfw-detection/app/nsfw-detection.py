import argparse
import os

import numpy as np

from aperturedb import Utils
from aperturedb import QueryGenerator
from aperturedb import Connector
from aperturedb import ParallelQuery

from NSFWDetector import NSFWDetector


class FindImageQueryGenerator(QueryGenerator.QueryGenerator):

    """
        Generates n FindImage Queries
    """

    def __init__(self, db, max_images=0, batch_size=16):

        self.db = db

        self.detector = NSFWDetector("input/saved_model.h5")

        query = [{
            "FindImage": {
                "constraints": {
                    "wf_nsfw_detector_neutral": ["==", None]
                },
                "results": {
                    "count": True
                }
            }
        }]

        response, _ = db.query(query)

        try:
            total_images = response[0]["FindImage"]["count"]
        except:
            print("Error retrieving the number of images. No images in the db?")
            exit(0)

        if total_images == 0:
            print("No images in the database. Bye!")
            exit(0)

        print(f"total_images: {total_images}")

        self.batch_size = batch_size
        self.total_batches = int(total_images / self.batch_size)

        if max_images > 0:
            self.len = min(self.total_batches, max_images//self.batch_size)

    def __len__(self):
        return self.len

    def getitem(self, idx):

        if idx < 0 or self.len <= idx:
            return None

        query = [{
            "FindImage": {
                "blobs": True,
                "constraints": {
                    "wf_nsfw_detector_neutral": ["==", None]
                },
                "batch": {
                    "batch_size": self.batch_size,
                    "batch_id": idx
                },
                "operations": [
                    {
                        "type": "resize",
                        "width": 224,
                        "height": 224
                    }
                ],
                "results": {
                    "list": ["_uniqueid"]
                }
            }
        }]

        return query, []

    def response_handler(self, query, blobs, response, r_blobs):

        try:
            uniqueids = [i["_uniqueid"] for i in response[0]["FindImage"]["entities"]]
        except:
            print(f"error: {response}")
            return 0

        predictions = self.detector.infer_batch(r_blobs)

        query = []

        for prediction, uniqueid in zip(predictions, uniqueids):

            query.append({
                "UpdateImage": {
                    "constraints": {
                        "_uniqueid": ["==", uniqueid]
                    },
                    "properties": {
                        "wf_nsfw_detector_neutral": prediction["neutral"],
                        "wf_nsfw_detector_hentai": prediction["hentai"],
                        "wf_nsfw_detector_porn": prediction["porn"],
                        "wf_nsfw_detector_sexy": prediction["sexy"],
                    }
                }
            })

        db = self.db.clone()

        r, _ = db.query(query)

        if not db.last_query_ok():
            db.print_last_response()

def clean_annotations(db):

    print("Cleaning Embeddings...")

    db.query([{
        "UpdateImage": {
            "constraints": {
                "wf_nsfw_detector_neutral": ["!=", None]
            },
            "remove_props": [
                "wf_nsfw_detector_neutral",
                "wf_nsfw_detector_hentai",
                "wf_nsfw_detector_porn",
                "wf_nsfw_detector_sexy",
            ]
        }
    }])

    db.print_last_response()

def main(params):

    db = Utils.create_connector()

    if params.clean:
        clean_annotations(db)

    generator = FindImageQueryGenerator(db, params.max_retrieved)
    querier = ParallelQuery.ParallelQuery(db)

    print("Running Detector...")

    querier.query(generator,
                    numthreads=params.numthreads,
                    stats=True)

    print("Done.")


def get_args():

    def str2bool(v):
        if isinstance(v, bool):
            return v
        if v.lower() in ('yes', 'true', 't', 'y', '1'):
            return True
        elif v.lower() in ('no', 'false', 'f', 'n', '0'):
            return False
        else:
            raise argparse.ArgumentTypeError('Boolean value expected.')

    obj = argparse.ArgumentParser()

    obj.add_argument('-numthreads', type=int,
                     default=os.environ.get('NUMTHREADS', 4))

    obj.add_argument('-clean',  type=str2bool,
                     default=os.environ.get('CLEAN', "false"))

    # For testing
    obj.add_argument('-max_retrieved',  type=int,
                     default=os.environ.get('MAX_RETRIEVED', 4096))

    params = obj.parse_args()

    return params


if __name__ == "__main__":
    args = get_args()
    main(args)
