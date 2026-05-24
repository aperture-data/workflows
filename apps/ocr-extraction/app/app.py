import os
from typing import Literal
import logging

from aperturedb import CommonLibrary
from aperturedb import ParallelQuery
from embeddings import Embedder
from connection_pool import ConnectionPool
from ocr import OCR
from wf_argparse import ArgumentParser

from image_ocr import FindImageOCRQueryGenerator
from pdf_ocr import FindPDFOCRQueryGenerator

IMAGE_EXTRACTION_DESCRIPTOR_SET = 'wf_ocr_images'
IMAGE_EXTRACTION_DONE_PROPERTY = 'wf_ocr_done'
PDF_EXTRACTION_DESCRIPTOR_SET = 'wf_ocr_pdfs'
PDF_EXTRACTION_DONE_PROPERTY = 'wf_ocr_done'


logger = logging.getLogger(__name__)


def clean_embeddings(db):

    print("Cleaning Embeddings...")

    db.query([{
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
                IMAGE_EXTRACTION_DONE_PROPERTY: ["!=", None]
            },
            "remove_props": [IMAGE_EXTRACTION_DONE_PROPERTY]
        }
    }, {
        "UpdateBlob": {
            "constraints": {
                PDF_EXTRACTION_DONE_PROPERTY: ["!=", None]
            },
            "remove_props": [PDF_EXTRACTION_DONE_PROPERTY]
        }
    }, {
        "DeleteEntity": {
            "with_class": "ExtractedText",
        }
    },
    ])

    db.print_last_response()


def main(params):

    logging.basicConfig(level=params.log_level, force=True)

    logger.info(f"Starting OCR Extraction: {params}")

    pool = ConnectionPool()

    # Initialize OCR instance
    ocr = OCR.create(provider=params.ocr_method)

    if params.clean:
        with pool.get_connection() as db:
            clean_embeddings(db)

    if params.extract_image_text:
        with pool.get_connection() as db:
            embedder = Embedder.from_new_descriptor_set(
                db, IMAGE_EXTRACTION_DESCRIPTOR_SET,
                provider="clip",
                model_name=params.model_name,
                properties={"type": "text", "source_type": "image", "ocr_method": params.ocr_method})
        generator = FindImageOCRQueryGenerator(
            pool, embedder, done_property=IMAGE_EXTRACTION_DONE_PROPERTY, ocr=ocr, generate_embeddings=params.generate_embeddings)
        with pool.get_connection() as db:
            querier = ParallelQuery.ParallelQuery(db)

        print("Running Image Text Extraction...")

        querier.query(generator, batchsize=1,
                      numthreads=params.numthreads,
                      stats=True)

        print("Done with Image Text Extraction.")

    if params.extract_pdf_text:
        with pool.get_connection() as db:
            embedder = Embedder.from_new_descriptor_set(
                db, PDF_EXTRACTION_DESCRIPTOR_SET,
                provider="clip",
                model_name=params.model_name,
                properties={"type": "text", "source_type": "pdf", "ocr_method": params.ocr_method})
        generator = FindPDFOCRQueryGenerator(
            pool, embedder, done_property=PDF_EXTRACTION_DONE_PROPERTY, ocr=ocr, generate_embeddings=params.generate_embeddings)
        with pool.get_connection() as db:
            querier = ParallelQuery.ParallelQuery(db)

        print("Running PDF OCR Extraction...")

        querier.query(generator, batchsize=1,
                      numthreads=params.numthreads,
                      stats=True)

        print("Done with PDF OCR Extraction.")

    print("Done")


def get_args():
    obj = ArgumentParser(support_legacy_envars=True)
    obj.add_argument('-numthreads', type='non_negative_int', default=4)
    obj.add_argument('-model_name',  type='clip_model_name', default='ViT-B/16')
    obj.add_argument('-clean',  type='bool', default=False)
    obj.add_argument('--extract-image-text', type='bool', default=False)
    obj.add_argument('--extract-pdf-text', type='bool', default=False)
    obj.add_argument('--ocr-method', type='ocr_method', default='tesseract')
    obj.add_argument('--log-level', type='log_level', default='WARNING')
    obj.add_argument('--generate-embeddings', type='bool', default=False)

    params = obj.parse_args()

    if not (any([params.extract_image_text, params.extract_pdf_text])):
        raise ValueError("No extractions specified")

    return params


if __name__ == "__main__":
    args = get_args()
    main(args)
