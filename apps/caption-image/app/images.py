import io
import math
import logging
import threading

from PIL import Image

from aperturedb import QueryGenerator

import torch
from transformers import AutoProcessor, BlipForConditionalGeneration

logger = logging.getLogger(__name__)

# Lazy-loaded globals
_processor = None
_model = None
_model_lock = threading.Lock()

def get_model_and_processor():
    global _processor, _model
    if _processor is None or _model is None:
        with _model_lock:
            if _processor is None or _model is None:
                _processor = AutoProcessor.from_pretrained("Salesforce/blip-image-captioning-base")
                _model = BlipForConditionalGeneration.from_pretrained("Salesforce/blip-image-captioning-base")
                _model.eval()
    return _processor, _model


class FindImageQueryGenerator(QueryGenerator.QueryGenerator):

    """
        Generates n FindImage Queries
    """

    def __init__(self, pool, caption_image_property: str, batch_size: int = 32):

        self.pool = pool
        self.caption_image_property = caption_image_property

        query = [{
            "FindImage": {
                "constraints": {
                    self.caption_image_property + "_done": ["!=", True]
                },
                "results": {
                    "list": ["_uniqueid"]
                }
            }
        }]

        status, response, _ = self.pool.execute_query(query)

        try:
            self.unique_ids = [i["_uniqueid"] for i in response[0]["FindImage"]["entities"]]
            total_images = len(self.unique_ids)
        except Exception as e:
            logger.error(f"Error retrieving the images. No images in the db? {e}")
            exit(0)

        if total_images == 0:
            logger.warning("No images to be processed. Continuing!")

        logger.info(f"Total images to process: {total_images}")

        self.batch_size = batch_size
        self.total_batches = int(math.ceil(total_images / self.batch_size))

        self.len = self.total_batches

    def __len__(self):
        return self.len

    def getitem(self, idx):

        if idx < 0 or self.len <= idx:
            return None

        start_idx = idx * self.batch_size
        end_idx = start_idx + self.batch_size
        batch_ids = self.unique_ids[start_idx:end_idx]

        query = [{
            "FindImage": {
                "blobs": True,
                "constraints": {
                    "_uniqueid": ["in", batch_ids]
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
            logger.exception(f"error: {response}")
            return 0

        captions = []
        processor, model = get_model_and_processor()
        for b in r_blobs:
            image = Image.open(io.BytesIO(b))
            text = "A picture of"
            inputs = processor(images=image, text=text, return_tensors="pt")
            with torch.no_grad():
                output = model.generate(**inputs)
            caption = processor.decode(output[0], skip_special_tokens=True)
            captions.append(caption)

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
                        self.caption_image_property: captions[i],
                        self.caption_image_property + "_done": True
                    },
                }
            })



        status, r, _ = self.pool.execute_query(query)
        if status != 0:
            logger.error(f"Query failed: {r}")
