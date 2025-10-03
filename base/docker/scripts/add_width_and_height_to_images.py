from aperturedb.QueryGenerator import QueryGenerator
from aperturedb.ParallelQuery import ParallelQuery
from typing import Optional
from connection_pool import ConnectionPool
import math
from PIL import Image
import io
import os
from wf_argparse import ArgumentParser
import logging

logger = logging.getLogger(__name__)

def get_width_and_height(b: bytes) -> tuple[int, int]:
    pil_image = Image.open(io.BytesIO(b))
    return pil_image.width, pil_image.height

# define an exception to use when no images to process
class NoImagesToProcessException(Exception):
    pass

class AddWidthAndHeightToImages(QueryGenerator):
    """This class adds width and height properties to Image objects that lack them."""
    def __init__(self, pool: ConnectionPool, 
        width_property: str = "width",
        height_property: str = "height",
        additional_contraints: Optional[dict] = None,
        batch_size: int = 1,
        ):
        self.pool = pool
        self.width_property = width_property
        self.height_property = height_property
        self.additional_contraints = additional_contraints if additional_contraints is not None else {}
        self.batch_size = batch_size

        query = [
            {
                "FindImage": {
                    "constraints": {
                        self.width_property: ["==", None],
                        self.height_property: ["==", None],
                        **self.additional_contraints
                    },
                    "results": {
                        "count": True
                    }
                }
            }
        ]

        status, response, _ = self.pool.execute_query(query)
        if status != 0:
            raise ValueError(f"Error finding images with no width or height: {response}")

        try:
            total_images = response[0]["FindImage"]["count"]
        except:
            logger.exception("Error getting total images:")
            self.pool.print_last_response()
            raise

        if total_images == 0:
            logger.warning("No images to process.")
            raise NoImagesToProcessException("No images to process.")

        self.total_batches = int(math.ceil(total_images / self.batch_size))

        self.len = self.total_batches

    def __len__(self):
        return self.len

    def getitem(self, idx):
        if idx < 0 or self.len <= idx:
            return None

        query = [
            {
                "FindImage": {
                    "blobs": True,
                    "constraints": {
                        self.width_property: ["==", None],
                        self.height_property: ["==", None],
                        **self.additional_contraints
                    },
                    "batch": {
                        "batch_size": self.batch_size,
                        "batch_id": idx
                    },
                    "results": {
                        "list": ["_uniqueid"]
                    }
                }
            }
        ]

        return query, []

    def response_handler(self, query, blobs, response, r_blobs):
        try:
            uniqueids = [i["_uniqueid"]
                         for i in response[0]["FindImage"]["entities"]]
        except:
            logger.exception(f"error: {response}")
            return 0

        query2 = []

        for uid, b in zip(uniqueids, r_blobs):
            width, height = get_width_and_height(b)
            image_ref = len(query2) + 1
            query2.append({
                "FindImage": {
                    "_ref": image_ref,
                    "constraints": {
                        "_uniqueid": ["==", uid]
                    }
                }
            })
            query2.append({
                "UpdateImage": {
                    "ref": image_ref,
                    "properties": {
                        self.width_property: width,
                        self.height_property: height
                    }
                }
            })

        status, response, _ = self.pool.execute_query(query2)
        if status != 0:
            logger.error(f"Error updating images: {response}")
            raise ValueError(f"Error updating images: {response}")

def add_width_and_height_to_images(
    pool: ConnectionPool, 
    width_property: str = "width",
    height_property: str = "height",
    additional_contraints: Optional[dict] = None,
    batch_size: int = 1,
    numthreads: int = 1,
):
    try:
        generator = AddWidthAndHeightToImages(pool, 
            width_property=width_property, 
            height_property=height_property, 
            additional_contraints=additional_contraints, 
            batch_size=batch_size)
    except NoImagesToProcessException:
        logger.warning("No images to process. Skipping.")
        return

    with pool.get_connection() as db:
        querier = ParallelQuery(db)

    logger.info("Running Add Width and Height to Images...")
    querier.query(generator, batchsize=batch_size, numthreads=numthreads, stats=True)
    logger.info("Done.")

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--batch_size", type=int, default=32)
    parser.add_argument("--numthreads", type=int, default=10)
    parser.add_argument("--log-level", type=str, default="WARNING")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level.upper(), force=True)

    pool = ConnectionPool()
    add_width_and_height_to_images(pool, batch_size=args.batch_size, numthreads=args.numthreads)