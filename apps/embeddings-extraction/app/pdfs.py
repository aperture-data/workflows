from text_extraction.text_extractor import TextExtractor
from text_extraction.segmentation import TextSegmenter
from text_extraction.schema import TextBlock, Segment

import math
import logging
from itertools import islice

import torch
import numpy as np
import clip
from typing import Iterable, Tuple, List

from aperturedb import QueryGenerator

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

    def __init__(self, db, descriptor_set: str, model_name):

        self.db = db
        self.model_name = model_name
        self.descriptor_set = descriptor_set

        # Choose the model to be used.
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model, self.preprocess = clip.load(model_name, device=self.device)

        query = [{
            "FindBlob": {
                "constraints": {
                    "document_type": ["==", "pdf"],
                    "wf_embeddings_clip": ["!=", True]
                },
                "results": {
                    "count": True
                }
            }
        }]

        response, _ = db.query(query)

        try:
            total_pdfs = response[0]["FindBlob"]["count"]
        except:
            logger.error(
                "Error retrieving the number of PDFs. No PDFs in the db?")
            exit(0)

        if total_pdfs == 0:
            logger.warning("No PDFs to be processed. Bye!")
            exit(0)

        logger.info(f"Total PDFs to process: {total_pdfs}")

        self.batch_size = 32
        self.total_batches = int(math.ceil(total_pdfs / self.batch_size))

        self.len = self.total_batches

        self.extractor = TextExtractor()
        self.segmenter = TextSegmenter(max_tokens=MAX_TOKENS,
                                       overlap_tokens=OVERLAP_TOKENS)

        self.model, self.preprocess = clip.load(
            model_name, device=self.device)
        self.model.eval()
        self.tokenizer = clip.tokenize

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
                    "list": ["_uniqueid"]
                }
            }
        }]

        return query, []

    def segments_to_embeddings(self, segments: Iterable[Segment]) -> List[bytes]:
        """
        Convert segments to embeddings using the CLIP model.
        """
        texts = [segment.text for segment in segments]
        # Specifying truncate=True means that overlong texts will be silently truncated, which is better than failing.
        logger.info(
            f"Tokenizing {len(texts)} segments for embedding generation.")
        tokens = self.tokenizer(texts, truncate=True).to(self.device)

        with torch.no_grad():
            logger.info("Generating embeddings for segments.")
            vectors = self.model.encode_text(tokens)
            logger.info("Converting embeddings to byte format.")
            vectors = [
                vector.float().detach().cpu().numpy().tobytes()
                for vector in vectors]

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

        for segment_batch in batch(segments, 100):
            logger.info(f"Processing batch of {len(segment_batch)} segments.")
            embeddings = self.segments_to_embeddings(segment_batch)
            logger.info(
                f"Generated {len(embeddings)} embeddings for segments.")
            yield from zip(segment_batch, embeddings)

    def response_handler(self, query, blobs, response, r_blobs):

        try:
            uniqueids = [i["_uniqueid"]
                         for i in response[0]["FindBlob"]["entities"]]
        except:
            logger.exception(f"error: {response}")
            return 0

        total_segments = 0
        for uniqueid, b in zip(uniqueids, r_blobs):
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

                for segment, embedding in segment_batch:
                    blob_segments += 1
                    properties = {}
                    if segment.title:
                        properties["title"] = segment.title
                    if segment.text:
                        properties["text"] = segment.text
                    properties["type"] = "text"
                    properties["total_tokens"] = segment.total_tokens
                    query.append({
                        "AddDescriptor": {
                            "set": self.descriptor_set,
                            "connect": {
                                "ref": 1
                            },
                            "properties": properties,
                        }
                    })
                    vectors.append(embedding)

                db = self.db.clone()
                r, _ = db.query(query, vectors)

                if not db.last_query_ok():
                    db.print_last_response()

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
                        "wf_embeddings_clip": True,
                        "segments": blob_segments,
                    },
                }
            })
            total_segments += blob_segments
            db = self.db.clone()
            r, _ = db.query(query, vectors)

            if not db.last_query_ok():
                db.print_last_response()
            logger.debug(
                f"Processed {blob_segments} segments for uniqueid {uniqueid}.")

        logger.info(f"Total segments processed: {total_segments}")
