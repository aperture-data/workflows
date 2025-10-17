import io
import math
import logging

from PIL import Image

from aperturedb import QueryGenerator
from connection_pool import ConnectionPool

from PIL import Image
from transformers import AutoProcessor, BlipForConditionalGeneration

processor = AutoProcessor.from_pretrained("Salesforce/blip-image-captioning-base")
model = BlipForConditionalGeneration.from_pretrained("Salesforce/blip-image-captioning-base")

logger = logging.getLogger(__name__)


class FindImageQueryGenerator(QueryGenerator.QueryGenerator):

    """
        Generates n FindImage Queries
    """

    def __init__(self, pool, caption_image_property: str):

        self.pool = pool
        self.caption_image_property = caption_image_property

        query = [{
            "FindImage": {
                "constraints": {
                    self.caption_image_property: ["==", None]
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
            logger.error("Error retrieving the number of images. No images in the db?")
            exit(0)

        if total_images == 0:
            logger.warning("No images to be processed. Continuing!")

        logger.info(f"Total images to process: {total_images}")

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
                    self.caption_image_property: ["==", None]
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
            logger.exception(f"error: {response}")
            return 0

        desc_blobs = []

        captions = []
        for b in r_blobs:
            image = Image.open(io.BytesIO(b))
            text = "A picture of"
            inputs = processor(images=image, text=text, return_tensors="pt")
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
                        self.caption_image_property: captions[i]
                    },
                }
            })



        self.pool.execute_query(query)