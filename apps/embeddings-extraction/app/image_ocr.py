import math

import torch
import cv2
import numpy as np
from PIL import Image
import clip

from aperturedb import QueryGenerator
from connection_pool import ConnectionPool
import pytesseract
from io import BytesIO
import logging
from embeddings import Embedder
from text_extraction.segmentation import TextSegmenter
from text_extraction.schema import TextBlock, Segment
from typing import Iterable, List

logger = logging.getLogger(__name__)

class FindImageOCRQueryGenerator(QueryGenerator.QueryGenerator):

    """
        Generates n FindImage Queries
    """

    def __init__(self, pool, embedder: Embedder, done_property: str, ocr):

        self.pool = pool
        self.embedder = embedder
        self.done_property = done_property
        self.ocr = ocr

        max_tokens = self.embedder.context_length
        overlap_tokens = min(max_tokens // 10, 10)
        self.segmenter = TextSegmenter(max_tokens=max_tokens,
                                       overlap_tokens=overlap_tokens, 
                                       min_tokens=None)

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
                # Full-size images are needed for OCR.
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
        query2 = []

        for uid, b in zip(uniqueids, r_blobs):
            image = Image.open(BytesIO(b)).convert("RGB")
            text = self.ocr.image_to_text(image)
            if text:
                logger.debug(f"Text: {text}")
                image_ref = len(query2) + 1
                text_ref = image_ref + 1
                query2.extend([
                    {
                        "FindImage": {
                            "_ref": image_ref,
                            "constraints": {
                                "_uniqueid": ["==", uid]
                            },
                        }
                    },
                    {
                        "UpdateImage": {
                            "ref": image_ref, 
                            "properties": {
                                self.done_property: True
                            },
                        }
                    },
                    {
                        "AddEntity": {
                            "class": "ImageExtractedText",
                            "properties": {
                                "text": text,
                                "type": "extracted_from_image",
                                "ocr_method": self.ocr.method,
                                "source_type": "image",
                            },  
                            "connect": {
                                "ref": image_ref,
                                "class": "imageHasExtractedText",
                            },
                            "_ref": text_ref,
                        }
                    }]
                )
                block = TextBlock(text=text)
                segments = list(self.segmenter.segment([block], clean_only=False))
                if not segments:
                    logger.warning(f"No segments found for text: {text}")
                    continue
                embeddings = list(self.segments_to_embeddings(segments))
                if len(embeddings) != len(segments):
                    logger.warning(f"Number of embeddings ({len(embeddings)}) does not match number of segments ({len(segments)}) for text: {text}")
                    continue

                for segment, embedding in zip(segments, embeddings):
                    segment_ref = len(query2) + 1
                    query2.extend([
                        {
                            "AddDescriptor": {
                                "set": self.embedder.descriptor_set,
                                "connect": {
                                    "ref": text_ref,
                                    "class": "extractedTextHasDescriptor",
                                },
                                "properties": {
                                    "text": segment.text,
                                    "type": "text",
                                    "extraction_type": "ocr",
                                    "ocr_method": self.ocr.method,
                                },
                                "_ref": segment_ref,
                            },
                        },
                        {
                            "AddConnection": {
                                "class": "imageHasDescriptor",
                                "src": image_ref,
                                "dst": segment_ref,
                            },
                        }
                    ])
                    desc_blobs.append(embedding)

        with self.pool.get_connection() as client:
            from aperturedb.CommonLibrary import execute_query
            status, r, _ = execute_query(client, query2, desc_blobs)
            assert status == 0, f"Query failed: {r}"

    def segments_to_embeddings(self, segments: Iterable[Segment]) -> List[bytes]:
        texts = [segment.text for segment in segments]
        vectors = self.embedder.embed_texts(texts)
        return [v.tobytes() for v in vectors]
