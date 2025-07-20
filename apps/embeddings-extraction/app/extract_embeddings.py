import os
import argparse
from typing import Optional

import clip

from aperturedb import CommonLibrary
from aperturedb import ParallelQuery


from images import FindImageQueryGenerator

IMAGE_DESCRIPTOR_SET = 'wf_embeddings_clip'
TEXT_DESCRIPTOR_SET = 'wf_embeddings_clip_text'


def clean_embeddings(db):

    print("Cleaning Embeddings...")

    db.query([{
        "DeleteDescriptorSet": {
            "with_name": "wf_embeddings_clip"
        }
    }, {
        "UpdateImage": {
            "constraints": {
                "wf_embeddings_clip": ["!=", None]
            },
            "remove_props": ["wf_embeddings_clip"]
        }
    }])

    db.print_last_response()


def add_descriptor_set(db,
                       descriptor_set: str,
                       properties: Optional[dict] = None):

    print("Adding Descriptor Set...")

    db.query([{
        "AddDescriptorSet": {
            "name": descriptor_set,
            "engine": "HNSW",
            "metric": "CS",
            "dimensions": 512,
            **({"properties": properties} if properties else {})
        }
    }])

    db.print_last_response()


def main(params):

    db = CommonLibrary.create_connector()

    if params.clean:
        clean_embeddings(db)

    if params.extract_images:
        add_descriptor_set(db,
                           descriptor_set=IMAGE_DESCRIPTOR_SET,
                           properties={"type": "image"})

        generator = FindImageQueryGenerator(db, params.model_name)
        querier = ParallelQuery.ParallelQuery(db)

        print("Running Images Detector...")

        querier.query(generator, batchsize=1,
                      numthreads=params.numthreads,
                      stats=True)

        print("Done with Images.")


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

    obj.add_argument('--extract-images', type=str2bool,
                     default=os.environ.get('WF_EXTRACT_IMAGES', False))

    obj.add_argument('--extract-pdfs', type=str2bool,
                     default=os.environ.get('WF_EXTRACT_PDFS', False))

    params = obj.parse_args()

    # >>> import clip
    # clip.available_models>>> clip.available_models()
    # ['RN50', 'RN101', 'RN50x4', 'RN50x16', 'RN50x64', 'ViT-B/32', 'ViT-B/16', 'ViT-L/14', 'ViT-L/14@336px']
    if params.model_name not in clip.available_models():
        raise ValueError(
            f"Invalid model name. Options: {clip.available_models()}")

    return params


if __name__ == "__main__":
    args = get_args()
    main(args)
