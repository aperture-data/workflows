import os
import argparse
from typing import Literal
import logging

import clip

from aperturedb import CommonLibrary
from aperturedb import ParallelQuery
from embeddings import Embedder
from connection_pool import ConnectionPool

from images import FindImageQueryGenerator
from pdfs import FindPDFQueryGenerator

IMAGE_DESCRIPTOR_SET = 'wf_embeddings_clip'
TEXT_DESCRIPTOR_SET = 'wf_embeddings_clip_text'
DONE_PROPERTY = 'wf_embeddings_clip'

def clean_embeddings(db):

    print("Cleaning Embeddings...")

    db.query([{
        "DeleteDescriptorSet": {
            "with_name": IMAGE_DESCRIPTOR_SET
        }
    }, {
        "DeleteDescriptorSet": {
            "with_name": TEXT_DESCRIPTOR_SET
        }
    }, {
        "UpdateImage": {
            "constraints": {
                DONE_PROPERTY: ["!=", None]
            },
            "remove_props": [DONE_PROPERTY]
        }
    }, {
        "UpdateBlob": {
            "constraints": {
                DONE_PROPERTY: ["!=", None]
            },
            "remove_props": [DONE_PROPERTY]
        }
    }])

    db.print_last_response()


def main(params):

    logging.basicConfig(level=params.log_level.upper())

    pool = ConnectionPool()

    if params.clean:
        with pool.get_connection() as db:
            clean_embeddings(db)

    if params.extract_images:
        with pool.get_connection() as db:
            embedder = Embedder.from_new_descriptor_set(
                db, IMAGE_DESCRIPTOR_SET,
                provider="clip",
                model_name=params.model_name,
                properties={"type": "image"})
        generator = FindImageQueryGenerator(
            pool, embedder, done_property=DONE_PROPERTY)
        with pool.get_connection() as db:
            querier = ParallelQuery.ParallelQuery(db)

        print("Running Images Detector...")

        querier.query(generator, batchsize=1,
                      numthreads=params.numthreads,
                      stats=True)

        print("Done with Images.")

    if params.extract_pdfs:
        with pool.get_connection() as db:
            embedder = Embedder.from_new_descriptor_set(
                db, TEXT_DESCRIPTOR_SET,
                provider="clip",
                model_name=params.model_name,
                properties={"type": "text", "source_type": "pdf"})
        generator = FindPDFQueryGenerator(
            pool, embedder, done_property=DONE_PROPERTY)
        with pool.get_connection() as db:
            querier = ParallelQuery.ParallelQuery(db)

        print("Running PDFs Detector...")

        querier.query(generator, batchsize=1,
                      numthreads=params.numthreads,
                      stats=True)

        print("Done with PDFs.")


    print("Done")


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

    obj.add_argument('--log-level', type=str,
                     default=os.environ.get('WF_LOG_LEVEL', 'WARNING'))

    params = obj.parse_args()

    # >>> import clip
    # clip.available_models>>> clip.available_models()
    # ['RN50', 'RN101', 'RN50x4', 'RN50x16', 'RN50x64', 'ViT-B/32', 'ViT-B/16', 'ViT-L/14', 'ViT-L/14@336px']
    if params.model_name not in clip.available_models():
        raise ValueError(
            f"Invalid model name. Options: {clip.available_models()}")

    if not (any([params.extract_images, params.extract_pdfs])):
        raise ValueError("No extractions specified")

    return params


if __name__ == "__main__":
    args = get_args()
    main(args)
