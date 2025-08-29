import math

import cv2
import numpy as np
from PIL import Image

from aperturedb import QueryGenerator
from connection_pool import ConnectionPool


class FindImageQueryGenerator(QueryGenerator.QueryGenerator):

    """
        Generates n FindImage Queries
    """

    def __init__(self, pool, embedder, done_property: str):

        self.pool = pool
        self.embedder = embedder
        self.done_property = done_property

        query = [{
            "FindImage": {
                "constraints": {
                    self.done_property: ["!=", True]
                },
                "results": {
                    "count": True
                }
            }
        }]

        _, response, _ = self.pool.execute_query(query)

        try:
            total_images = response[0]["FindImage"]["count"]
        except:
            print("Error retrieving the number of images. No images in the db?")
            exit(0)

        if total_images == 0:
            print("No images to be processed. Bye!")
            exit(0)

        print(f"Total images to process: {total_images}")

        self.batch_size = 32
        self.total_batches = int(math.ceil(total_images / self.batch_size))

        self.len = self.total_batches

    def __len__(self):
        return self.len

    def getitem(self, idx):

        if idx < 0 or self.len <= idx:
            return None

        query = [{
            "FindImage": {
                "blobs": True,
                "constraints": {
                    self.done_property: ["!=", True]
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
            uniqueids = [i["_uniqueid"]
                         for i in response[0]["FindImage"]["entities"]]
        except:
            print(f"error: {response}")
            return 0

        desc_blobs = []

        for b in r_blobs:
            image_features = self.embedder.embed_image(b)
            desc_blobs.append(image_features.tobytes())

        query = []
        for uniqueid, i in zip(uniqueids, range(len(uniqueids))):

            query.append({
                "FindImage": {
                    "_ref": i + 1,
                    "constraints": {
                        "_uniqueid": ["==", uniqueid]
                    },
                }
            })

            query.append({
                "UpdateImage": {
                    "ref": i + 1,
                    "properties": {
                        self.done_property: True
                    },
                }
            })

            query.append({
                "AddDescriptor": {
                    "set": self.embedder.descriptor_set,
                    "connect": {
                        "ref": i + 1
                    },
                    "properties": {
                        "type": "image",
                        "source_type": "image",
                        "extraction_type": "image"
                    }
                }
            })

        with self.pool.get_connection() as db:
            r, _ = db.query(query, desc_blobs)

            if not db.last_query_ok():
                db.print_last_response()
