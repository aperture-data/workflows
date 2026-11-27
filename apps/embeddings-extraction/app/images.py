from aperturedb.Constraints import predicate
import math
import logging

import cv2
import numpy as np
from PIL import Image

from aperturedb import QueryGenerator
from aperturedb_workflows.connection_pool import ConnectionPool

logger = logging.getLogger(__name__)


class FindImageQueryGenerator(QueryGenerator.QueryGenerator):

    """
        Generates n FindImage Queries
    """

    def __init__(self, pool, embedder, done_property: str):

        self.pool = pool
        self.embedder = embedder
        self.done_property = done_property

        query = [{'FindImage': {'constraints': predicate(self.done_property, '!=', True), 'results': {
            'list': ['_uniqueid']}, 'uniqueids': True}}]

        status, response, _ = self.pool.execute_query(query)
        if status != 0:
            raise RuntimeError(f"FindImage count query failed: {response}")

        try:
            selection = response[0]["FindImage"]
            if "entities" not in selection and selection.get("returned") != 0:
                raise RuntimeError(f"Invalid selection response: {response}")
            self._selected_ids = tuple(e["_uniqueid"] for e in selection.get("entities", []))
            total_images = len(self._selected_ids)
        except (KeyError, IndexError, TypeError) as exc:
            raise RuntimeError(
                f"Invalid FindImage count response: {response}") from exc

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

        query = [{'FindImage': {'blobs': True,
                                'constraints': predicate('_uniqueid',
                                                         'in',
                                                         self._selected_ids[idx * self.batch_size:(idx + 1) * self.batch_size]),
                                'operations': [{'type': 'resize',
                                                'width': 224,
                                                'height': 224}],
                                'results': {'list': ['_uniqueid']},
                                'uniqueids': True}}]

        return query, []

    def response_handler(self, query, blobs, response, r_blobs):

        try:
            uniqueids = [i["_uniqueid"]
                         for i in response[0]["FindImage"]["entities"]]
        except BaseException:
            logger.exception(f"error: {response}")
            return 0

        desc_blobs = []

        for b in r_blobs:
            image_features = self.embedder.embed_image(b)
            desc_blobs.append(image_features.tobytes())

        query = []
        for uniqueid, i in zip(uniqueids, range(len(uniqueids))):

            query.append(
                {'FindImage': {'_ref': i + 1, 'constraints': predicate('_uniqueid', '==', uniqueid)}})

            query.append({
                "UpdateImage": {
                    "ref": i + 1,
                    "properties": {
                        self.done_property: True
                    },
                }
            })

            query.append(
                {
                    'AddDescriptor': {
                        'class': self.embedder.descriptor_set,
                        'connect': {
                            'ref': i + 1,
                            'class': 'HasDescriptor'},
                        'properties': {
                            'type': 'image',
                            'source_type': 'image',
                            'extraction_type': 'image'}}})

        with self.pool.get_connection() as db:
            r, _ = db.query(query, desc_blobs)

            if not db.last_query_ok():
                db.print_last_response()
