import math

import torch
import cv2
import numpy as np
from PIL import Image
import clip

from aperturedb import QueryGenerator
from connection_pool import ConnectionPool


class FindImageQueryGenerator(QueryGenerator.QueryGenerator):

    """
        Generates n FindImage Queries
    """

    def __init__(self, db, descriptor_set: str, model_name: str):

        self.pool = ConnectionPool(connection_factory=db.clone)
        self.descriptor_set = descriptor_set

        # Choose the model to be used.
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model, self.preprocess = clip.load(model_name, device=self.device)

        query = [{
            "FindImage": {
                "constraints": {
                    "wf_embeddings_clip": ["!=", True]
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
                    "wf_embeddings_clip": ["!=", True]
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
            nparr = np.frombuffer(b, np.uint8)
            image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
            image = self.preprocess(Image.fromarray(
                image)).unsqueeze(0).to(self.device)

            image_features = self.model.encode_image(image)

            if self.device == "cuda":
                image_features = image_features.float()
                desc_blobs.append(
                    image_features.detach().cpu().numpy().tobytes())
            else:
                desc_blobs.append(image_features.detach().numpy().tobytes())

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
                        "wf_embeddings_clip": True
                    },
                }
            })

            query.append({
                "AddDescriptor": {
                    "set": self.descriptor_set,
                    "connect": {
                        "ref": i + 1
                    },
                    "properties": {
                        "type": "image"
                    }
                }
            })

        with self.pool.get_connection() as db:
            r, _ = db.query(query, desc_blobs)

            if not db.last_query_ok():
                db.print_last_response()
