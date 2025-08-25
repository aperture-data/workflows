import math

import torch
import cv2
import numpy as np
from PIL import Image
import clip

from aperturedb import QueryGenerator
from connection_pool import ConnectionPool
import pytesseract
from PIL import Image
from io import BytesIO
import logging
from embeddings import Embedder
# from text_segmenter import TextSegmenter

MAX_TOKENS = 1024
OVERLAP_TOKENS = 128

logger = logging.getLogger(__name__)

class FindImageOCRQueryGenerator(QueryGenerator.QueryGenerator):

    """
        Generates n FindImage Queries
    """

    def __init__(self, db, descriptor_set: str, model_name: str, done_property: str):

        self.pool = ConnectionPool(connection_factory=db.clone)
        self.descriptor_set = descriptor_set

        # Choose the model to be used.
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model, self.preprocess = clip.load(model_name, device=self.device)
        self.done_property = done_property
        # self.segmenter = TextSegmenter(max_tokens=MAX_TOKENS,
        #                                overlap_tokens=OVERLAP_TOKENS)
        # self.embedder = Embedder(model_name=model_name)

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
            text = pytesseract.image_to_string(image)
            if text:
                logger.debug(f"Text: {text}")
                ref = len(query2) + 1
                query2.extend([
                    {
                        "FindImage": {
                            "_ref": ref,
                            "constraints": {
                                "_uniqueid": ["==", uid]
                            },
                        }
                    },
                    {
                        "UpdateImage": {
                            "ref": ref, # This line was causing the error - ref not defined
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
                            },  
                            "connect": {
                                "ref": ref,
                                "class": "imageHasExtractedText",
                            }
                        }
                    }]
                )
                # block = TextBlock(text=text)  # This line was causing the error - TextBlock not imported
                # segments = self.segmenter.segment([block])
                

        with self.pool.get_connection() as client:
            from aperturedb.CommonLibrary import execute_query
            status, r, _ = execute_query(client, query2, desc_blobs)
            assert status == 0, f"Query failed: {r}"


