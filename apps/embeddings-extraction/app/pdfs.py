from text_extraction.text_extractor import TextExtractor
from text_extraction.segmentation import TextSegmenter
from text_extraction.schema import TextBlock, Segment

import math
import logging
from itertools import islice

from typing import Iterable, Tuple, List

from aperturedb import QueryGenerator
from connection_pool import ConnectionPool

logger = logging.getLogger(__name__)

# Ideally we want paragraphs about 500 tokens long,
# but we have to deal with the fact that CLIP has a maximum token limit.
# This might be less of a problem in the future, if we move to OpenCLIP.
MAX_TOKENS = 100
OVERLAP_TOKENS = 10


def batch(iterable, n=1):
    """Batch data into chunks of size n."""
    it = iter(iterable)  # Get an iterator from the input iterable
    while True:
        batch = tuple(islice(it, n))
        if not batch:  # If islice returns an empty tuple, the iterable is exhausted
            break
        yield batch


class FindPDFQueryGenerator(QueryGenerator.QueryGenerator):

    """
        Generates n FindBlob Queries
    """

    def __init__(self, pool, embedder, done_property: str):

        self.pool = pool
        self.embedder = embedder
        self.done_property = done_property

        query = [{
            "FindBlob": {
                "constraints": {
                    "document_type": ["==", "pdf"],
                    self.done_property: ["!=", True]
                },
                "results": {
                    "count": True
                }
            }
        }]

        _, response, _ = self.pool.execute_query(query)

        try:
            total_pdfs = response[0]["FindBlob"]["count"]
        except:
            logger.error(
                "Error retrieving the number of PDFs. No PDFs in the db?")
            exit(0)

        if total_pdfs == 0:
            logger.warning("No PDFs to be processed. Continuing!")


        logger.info(f"Total PDFs to process: {total_pdfs}")

        self.batch_size = 32
        self.total_batches = int(math.ceil(total_pdfs / self.batch_size))

        self.len = self.total_batches

        self.extractor = TextExtractor()
        self.segmenter = TextSegmenter(max_tokens=MAX_TOKENS,
                                       overlap_tokens=OVERLAP_TOKENS)

    def __len__(self):
        return self.len

    def getitem(self, idx):

        if idx < 0 or self.len <= idx:
            return None

        query = [{
            "FindBlob": {
                "blobs": True,
                "constraints": {
                    "document_type": ["==", "pdf"],
                    "wf_embeddings_clip": ["!=", True]
                },
                "batch": {
                    "batch_size": self.batch_size,
                    "batch_id": idx
                },
                "results": {
                    "list": ["_uniqueid", "url"]
                }
            }
        }]

        return query, []

    def segments_to_embeddings(self, segments: Iterable[Segment]) -> List[bytes]:
        """
        Convert segments to embeddings using the embedder.
        """
        texts = [segment.text for segment in segments]
        logger.info(f"Generating embeddings for {len(texts)} segments.")

        vectors = self.embedder.embed_texts(texts)
        logger.info("Converting embeddings to byte format.")
        vectors = [vector.tobytes() for vector in vectors]

        return vectors

    def blob_to_embeddings(self, blob: bytes) -> Iterable[Tuple[Segment, bytes]]:
        blocks = self.extractor.extract_blocks(
            data=blob,
            content_type="application/pdf"
        )

        text_blocks = [
            block
            for block in blocks
            if isinstance(block, TextBlock)
        ]

        if not text_blocks:
            logger.warning("No text blocks found in PDF.")
            return

        logger.info(f"Extracted {len(text_blocks)} text blocks from PDF.")

        segments = self.segmenter.segment(text_blocks)

        segment_number = 0
        for segment_batch in batch(segments, 100):
            logger.info(f"Processing batch of {len(segment_batch)} segments.")
            embeddings = self.segments_to_embeddings(segment_batch)
            logger.info(
                f"Generated {len(embeddings)} embeddings for segments.")
            numbers = range(segment_number, segment_number +
                            len(segment_batch))
            yield from zip(segment_batch, embeddings, numbers)
            segment_number += len(segment_batch)

    def response_handler(self, query, blobs, response, r_blobs):

        try:
            uniqueids = [i["_uniqueid"]
                         for i in response[0]["FindBlob"]["entities"]]
            urls = [i["url"] if "url" in i else None
                    for i in response[0]["FindBlob"]["entities"]]
        except:
            logger.exception(f"error: {response}")
            return 0

        total_segments = 0
        for uniqueid, url, b in zip(uniqueids, urls, r_blobs):
            blob_segments = 0
            segments_and_embeddings = self.blob_to_embeddings(b)
            for segment_batch in batch(segments_and_embeddings, 100):
                query = []
                vectors = []
                query.append({
                    "FindBlob": {
                        "_ref": 1,
                        "constraints": {
                            "_uniqueid": ["==", uniqueid]
                        },
                    }
                })

                for segment, embedding, number in segment_batch:
                    blob_segments += 1
                    properties = {}
                    if segment.title:
                        properties["title"] = segment.title
                    if segment.text:
                        properties["text"] = segment.text
                    properties["type"] = "text"
                    properties["source_type"] = "pdf"
                    properties["total_tokens"] = segment.total_tokens
                    properties["segment_number"] = number
                    properties["extraction_type"] = "text"

                    page_number = segment.page_number()
                    if page_number is not None:
                        properties["page_number"] = page_number

                    if url is not None:
                        segment_url = segment.url(url)
                        properties["url"] = segment_url

                    query.append({
                        "AddDescriptor": {
                            "set": self.embedder.descriptor_set,
                            "connect": {
                                "ref": 1
                            },
                            "properties": properties,
                        }
                    })
                    vectors.append(embedding)

                _, r, _ = self.pool.execute_query(query, vectors)

            # Finally mark the blob as processed
            query = []
            query.append({
                "FindBlob": {
                    "_ref": 1,
                    "constraints": {
                        "_uniqueid": ["==", uniqueid]
                    },
                }
            })
            query.append({
                "UpdateBlob": {
                    "ref": 1,
                    "properties": {
                        self.done_property: True,
                        "segments": blob_segments,
                    },
                }
            })
            total_segments += blob_segments
            _, r, _ = self.pool.execute_query(query)

            logger.debug(
                f"Processed {blob_segments} segments for uniqueid {uniqueid}.")

        logger.info(f"Total segments processed: {total_segments}")
