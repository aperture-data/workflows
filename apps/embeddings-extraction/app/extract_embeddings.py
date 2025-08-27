import os
import argparse
from typing import Optional
import logging

import clip

from aperturedb import CommonLibrary
from aperturedb import ParallelQuery
from embeddings import Embedder
from connection_pool import ConnectionPool


from images import FindImageQueryGenerator
from pdfs import FindPDFQueryGenerator
from image_ocr import FindImageOCRQueryGenerator
from pdf_ocr import FindPDFOCRQueryGenerator

IMAGE_DESCRIPTOR_SET = 'wf_embeddings_clip'
TEXT_DESCRIPTOR_SET = 'wf_embeddings_clip_text'
DONE_PROPERTY = 'wf_embeddings_clip'
IMAGE_EXTRACTION_DESCRIPTOR_SET = 'wf_embeddings_clip_image_extraction'
IMAGE_EXTRACTION_DONE_PROPERTY = 'wf_embeddings_clip_image_extraction_done'
PDF_EXTRACTION_DESCRIPTOR_SET = 'wf_embeddings_clip_pdf_extraction'
PDF_EXTRACTION_DONE_PROPERTY = 'wf_embeddings_clip_pdf_extraction_done'

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
        "DeleteDescriptorSet": {
            "with_name": IMAGE_EXTRACTION_DESCRIPTOR_SET
        }
    }, {
        "DeleteDescriptorSet": {
            "with_name": PDF_EXTRACTION_DESCRIPTOR_SET
        }
    }, {
        "UpdateImage": {
            "constraints": {
                DONE_PROPERTY: ["!=", None]
            },
            "remove_props": [DONE_PROPERTY]
        }
    }, {
        "UpdateImage": {
            "constraints": {
                IMAGE_EXTRACTION_DONE_PROPERTY: ["!=", None]
            },
            "remove_props": [IMAGE_EXTRACTION_DONE_PROPERTY]
        }
    }, {
        "UpdateBlob": {
            "constraints": {
                DONE_PROPERTY: ["!=", None]
            },
            "remove_props": [DONE_PROPERTY]
        }
    }, {
        "UpdateBlob": {
            "constraints": {
                PDF_EXTRACTION_DONE_PROPERTY: ["!=", None]
            },
            "remove_props": [PDF_EXTRACTION_DONE_PROPERTY]
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

    logging.basicConfig(level=params.log_level.upper())

    pool = ConnectionPool()

    if params.clean:
        clean_embeddings(db)

    if params.extract_images:
        add_descriptor_set(db,
                           descriptor_set=IMAGE_DESCRIPTOR_SET,
                           properties={"type": "image"})

        generator = FindImageQueryGenerator(
            pool, IMAGE_DESCRIPTOR_SET, params.model_name,
            done_property=DONE_PROPERTY)
        querier = ParallelQuery.ParallelQuery(db)

        print("Running Images Detector...")

        querier.query(generator, batchsize=1,
                      numthreads=params.numthreads,
                      stats=True)

        print("Done with Images.")

    if params.extract_pdfs:
        add_descriptor_set(db,
                           descriptor_set=TEXT_DESCRIPTOR_SET,
                           properties={"type": "text"})

        generator = FindPDFQueryGenerator(
            pool, TEXT_DESCRIPTOR_SET, params.model_name,
            done_property=DONE_PROPERTY)
        querier = ParallelQuery.ParallelQuery(db)

        print("Running PDFs Detector...")

        querier.query(generator, batchsize=1,
                      numthreads=params.numthreads,
                      stats=True)

        print("Done with PDFs.")

    if params.extract_image_text:
        embedder = Embedder.from_new_descriptor_set(
            db, IMAGE_EXTRACTION_DESCRIPTOR_SET,
            provider="clip",
            model_name=params.model_name)
        generator = FindImageOCRQueryGenerator(
            pool=pool, embedder=embedder, done_property=IMAGE_EXTRACTION_DONE_PROPERTY)
        querier = ParallelQuery.ParallelQuery(db)

        print("Running Image Text Extraction...")

        querier.query(generator, batchsize=1,
                      numthreads=params.numthreads,
                      stats=True)

        print("Done with Image Text Extraction.")

    if params.extract_pdf_text:
        embedder = Embedder.from_new_descriptor_set(
            db, PDF_EXTRACTION_DESCRIPTOR_SET,
            provider="clip",
            model_name=params.model_name)
        generator = FindPDFOCRQueryGenerator(
            pool=pool, embedder=embedder, done_property=PDF_EXTRACTION_DONE_PROPERTY)
        querier = ParallelQuery.ParallelQuery(db)

        print("Running PDF OCR Extraction...")

        querier.query(generator, batchsize=1,
                      numthreads=params.numthreads,
                      stats=True)

        print("Done with PDF OCR Extraction.")

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

    obj.add_argument('--extract-image-text', type=str2bool,
                     default=os.environ.get('WF_EXTRACT_IMAGE_TEXT', False))

    obj.add_argument('--extract-pdf-text', type=str2bool,
                     default=os.environ.get('WF_EXTRACT_PDF_TEXT', False))

    obj.add_argument('--log-level', type=str,
                     default=os.environ.get('WF_LOG_LEVEL', 'WARNING'))

    params = obj.parse_args()

    # >>> import clip
    # clip.available_models>>> clip.available_models()
    # ['RN50', 'RN101', 'RN50x4', 'RN50x16', 'RN50x64', 'ViT-B/32', 'ViT-B/16', 'ViT-L/14', 'ViT-L/14@336px']
    if params.model_name not in clip.available_models():
        raise ValueError(
            f"Invalid model name. Options: {clip.available_models()}")

    if not (any([params.extract_images, params.extract_pdfs, params.extract_image_text, params.extract_pdf_ocr])):
        raise ValueError("No extractions specified")

    return params


if __name__ == "__main__":
    args = get_args()
    main(args)
