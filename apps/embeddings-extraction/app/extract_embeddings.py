import os
import argparse
import math

import torch
import clip
import cv2
import numpy as np
from PIL import Image

from aperturedb import CommonLibrary
from aperturedb import QueryGenerator
from aperturedb import ParallelQuery


class FindImageQueryGenerator(QueryGenerator.QueryGenerator):

    """
        Generates n FindImage Queries
    """

    def __init__(self, db, model_name):

        self.db = db

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
            uniqueids = [i["_uniqueid"] for i in response[0]["FindImage"]["entities"]]
        except:
            print(f"error: {response}")
            return 0

        desc_blobs = []

        for b in r_blobs:
            nparr = np.frombuffer(b, np.uint8)
            image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
            image = self.preprocess(Image.fromarray(image)).unsqueeze(0).to(self.device)

            image_features = self.model.encode_image(image)

            if self.device == "cuda":
                image_features = image_features.float()
                desc_blobs.append(image_features.detach().cpu().numpy().tobytes())
            else:
                desc_blobs.append(image_features.detach().numpy().tobytes())

        query = []
        for  uniqueid, i in zip(uniqueids, range(len(uniqueids))):

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
                    "set": "wf_embeddings_clip",
                    "connect": {
                        "ref": i + 1
                    }
                }
            })

        # This is not nice, but we need to create a new connection
        # and this happens in parallel with many threads.
        db = self.db.create_new_connection()

        r, _ = db.query(query, desc_blobs)

        if not db.last_query_ok():
            db.print_last_response()

def clean_embeddings(db):

    print("Cleaning Embeddings...")

    db.query([{
        "DeleteDescriptorSet": {
            "with_name": "wf_embeddings_clip"
        }
    }, {
        "UpdateImage": {
            "constraints": {
                "_uniqueid": ["!=", ""]
            },
            "remove_props": ["wf_embeddings_clip"]
        }
    }])

    db.print_last_response()

def add_descriptor_set(db):

    print("Adding Descriptor Set...")

    db.query([{
        "AddDescriptorSet": {
            "name": "wf_embeddings_clip",
            "engine": "HNSW",
            "metric": "CS",
            "dimensions": 512,
        }
    }])

    db.print_last_response()

def main(params):

    db = CommonLibrary.create_connector()

    if params.clean:
        clean_embeddings(db)

    add_descriptor_set(db)

    generator = FindImageQueryGenerator(db, params.model_name)
    querier = ParallelQuery.ParallelQuery(db)

    print("Running Detector...")

    querier.query(generator, batchsize=1,
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

    obj.add_argument('-model_name',  type=str,
                     default=os.environ.get('MODEL_NAME', 'ViT-B/16'))

    obj.add_argument('-clean',  type=str2bool,
                     default=os.environ.get('CLEAN', "false"))

    params = obj.parse_args()

    # >>> import clip
    # clip.available_models>>> clip.available_models()
    # ['RN50', 'RN101', 'RN50x4', 'RN50x16', 'RN50x64', 'ViT-B/32', 'ViT-B/16', 'ViT-L/14', 'ViT-L/14@336px']
    if params.model_name not in clip.available_models():
        raise ValueError(f"Invalid model name. Options: {clip.available_models()}")

    return params


if __name__ == "__main__":
    args = get_args()
    main(args)
