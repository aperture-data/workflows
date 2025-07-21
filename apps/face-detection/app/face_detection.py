import os
import argparse
import io
import math
from PIL import Image

from aperturedb.CommonLibrary import create_connector, execute_query
from aperturedb import QueryGenerator
from aperturedb import ParallelQuery
from facenet_pytorch import MTCNN, InceptionResnetV1
import torch

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
mtcnn = MTCNN(image_size=96, margin=0, keep_all=True, device=device)
resnet = InceptionResnetV1(pretrained='vggface2', device=device).eval()

PROCESSED_LABEL_IMAGES = "wf_facenet_processed"
PROCESSED_LABEL_CONFIDENCE = "wf_facenet_confidence"


class FindImageQueryGenerator(QueryGenerator.QueryGenerator):
    """
    Generates FindImage Queries.
    """

    def __init__(self, db, *args, **kwargs):
        self.batch_size = kwargs.get("batch_size", 1)
        self.generate_embeddings = kwargs.get("generate_embeddings", False)
        self.db = db
        query = [{
            "FindImage": {
                "constraints": {
                    PROCESSED_LABEL_IMAGES: ["!=", True]
                },
                "results": {
                    "count": True
                }
            }
        }]

        result, response, _ = execute_query(db, query, [])
        try:
            total_images = response[0]["FindImage"]["count"]
        except:
            print("Error retrieving the number of images. No images in the db?")
            exit(0)

        print(f"Total images to process: {total_images}")

        self.batch_size = 1
        self.total_batches = math.ceil(total_images / self.batch_size)

    def __len__(self):
        return self.total_batches

    def getitem(self, idx):
        query = [{
            "FindImage": {
                "blobs": True,
                "constraints": {
                    PROCESSED_LABEL_IMAGES: ["!=", True]
                },
                "batch": {
                    "batch_size": self.batch_size,
                    "batch_id": idx
                },
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
        boxes = {}
        for i, b in enumerate(r_blobs):
            pil_image = Image.open(io.BytesIO(b))
            faces = mtcnn(pil_image)
            if faces is not None:
                embeddings = []
                for face in faces:
                    try:
                        if self.generate_embeddings:
                            embedding = resnet(face.unsqueeze(0).to(device))
                            serialized = embedding.cpu().detach().numpy().tobytes()
                            embeddings.append(serialized)
                            desc_blobs.append(serialized)
                        box, prob = mtcnn.detect(pil_image, landmarks=False)
                    except Exception as e:
                        print(f"Error: {e} for {i=}")
                        continue
                boxes[uniqueids[i]] = {
                    "index": i,
                    "boxes": list(zip(box.tolist(), prob.tolist()))
                }
                if self.generate_embeddings:
                    assert len(embeddings) == len(box)

        query = []

        ref_counter = 1
        for uniqueid in uniqueids:
            query.append({
                "FindImage": {
                    "_ref": ref_counter,
                    "constraints": {
                        "_uniqueid": ["==", uniqueid]
                    },
                }
            })

            query.append({
                "UpdateImage": {
                    "ref": ref_counter,
                    "properties": {
                        PROCESSED_LABEL_IMAGES: True
                    },
                }
            })

            bb_ref = 0
            if uniqueid in boxes:
                bb_ref = 1
                for box, prob in boxes[uniqueid]["boxes"]:
                    query.append({
                        "AddBoundingBox": {
                            "image_ref": ref_counter,
                            "_ref": ref_counter + bb_ref,
                            "label": "face",
                            "rectangle": {
                                "x": max(int(box[0]), 0),
                                "y": max(int(box[1]), 0),
                                "width": int(box[2] - box[0]),
                                "height": int(box[3] - box[1])
                            },
                            "properties": {
                                PROCESSED_LABEL_CONFIDENCE: prob
                            }
                        }
                    })
                    if self.generate_embeddings:
                        query.append({
                            "AddDescriptor": {
                                "set": PROCESSED_LABEL_IMAGES,
                                "connect": {
                                    "ref": ref_counter + bb_ref
                                }
                            }
                        })
                    bb_ref += 1
            ref_counter += (bb_ref + 1)

        # This is not nice, but we need to create a new connection
        # and this happens in parallel with many threads.
        db = self.db.clone()

        result, response, _ = execute_query(db, query, desc_blobs)
        if result != 0:
            print(f"Error: {response}")
            return 0


def clean_artifacts(db):
    query = [{
        "FindImage": {
            "_ref": 1,
            "constraints": {
                PROCESSED_LABEL_IMAGES: ["==", True]
            },
            "results": {
                "count": True
            }
        }
    }, {
        "UpdateImage": {
            "ref": 1,
            "remove_props": [PROCESSED_LABEL_IMAGES]
        }
    }, {
        "FindBoundingBox": {
            "_ref": 2,
            "constraints": {
                PROCESSED_LABEL_CONFIDENCE: ["!=", None]
            },
            "results": {
                "count": True
            }
        }
    }, {
        "DeleteBoundingBox": {
            "ref": 2
        }
    }, {
        "DeleteDescriptorSet": {
            "with_name": PROCESSED_LABEL_IMAGES
        }
    }]
    result, response, _ = execute_query(db, query, [])
    print(f"Cleaning Artifacts... {response if result!=0 else 'done'}")


def add_descriptor_set(db):

    result, response, _ = execute_query(db, [{
        "AddDescriptorSet": {
            "name": PROCESSED_LABEL_IMAGES,
            "engine": "HNSW",
            "metric": "CS",
            "dimensions": 512,
        }
    }], [], success_statuses=[0, 2])
    print(f"Added Descriptor Set... {response if result!=0 else 'done'}")


def main(params):

    db = create_connector()

    if params.clean:
        clean_artifacts(db)

    if params.generate_embeddings:
        add_descriptor_set(db)

    generator = FindImageQueryGenerator(db, batch_size=params.query_batchsize,
                                        generate_embeddings=params.generate_embeddings)
    print(f"Total Batches: {len(generator)}")
    querier = ParallelQuery.ParallelQuery(db)

    print("Running Detector...")

    querier.query(generator,
                  batchsize=1,
                  numthreads=params.query_numthreads,
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

    obj.add_argument('-query_numthreads', type=int,
                     default=os.environ.get('QUERY_NUMTHREADS', 1))

    obj.add_argument('-query_batchsize',  type=int,
                     default=os.environ.get('QUERY_BATCHSIZE', 1))

    obj.add_argument('-clean',  type=str2bool,
                     default=os.environ.get('CLEAN', "false"))

    obj.add_argument('-generate_embeddings', type=str2bool,
                     default=os.environ.get('GENERATE_EMBEDDINGS', "false"))

    params = obj.parse_args()
    return params


if __name__ == "__main__":
    args = get_args()
    print(args)
    main(args)
