import math
import logging
from itertools import islice
from typing import Iterable, Tuple, List, Optional

import torch
import numpy as np
from PIL import Image
from io import BytesIO
import fitz  # PyMuPDF
import uuid
from aperturedb import QueryGenerator
from connection_pool import ConnectionPool
from embeddings import Embedder
from text_extraction.segmentation import TextSegmenter
from text_extraction.schema import TextBlock, Segment


logger = logging.getLogger(__name__)

# Ideally we want paragraphs about 500 tokens long,
# but we have to deal with the fact that CLIP has a maximum token limit.
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


class FindPDFOCRQueryGenerator(QueryGenerator.QueryGenerator):

    """
        Generates n FindBlob Queries for PDFs that need OCR processing
    """

    def __init__(self, pool, embedder: Embedder, done_property: str, ocr, extract_embeddings: bool):

        self.pool = pool
        self.embedder = embedder
        self.done_property = done_property
        self.ocr = ocr
        self.extract_embeddings = extract_embeddings
        max_tokens = self.embedder.context_length
        overlap_tokens = min(max_tokens // 10, 10)
        self.segmenter = TextSegmenter(max_tokens=max_tokens,
                                       overlap_tokens=overlap_tokens,
                                       min_tokens=None)

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
            logger.warning("No PDFs to be processed. Bye!")
            exit(0)

        logger.info(f"Total PDFs to process: {total_pdfs}")

        self.batch_size = 32
        self.total_batches = int(math.ceil(total_pdfs / self.batch_size))

        self.len = self.total_batches

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
                    self.done_property: ["!=", True]
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

    def has_text_content(self, pdf_data: bytes) -> bool:
        """
        Check if PDF has extractable text content.
        Returns True if text can be extracted, False if it's image-only.
        """
        try:
            has_text = False
            with fitz.open(stream=pdf_data, filetype="pdf") as doc:
                for page_num in range(len(doc)):
                    page = doc.load_page(page_num)
                    text = page.get_text().strip()
                    if text:
                        has_text = True
                        break
            return has_text
        except Exception as e:
            logger.warning(f"Error checking PDF text content: {e}")
            return False

    def pdf_to_images(self, pdf_data: bytes) -> List[bytes]:
        """
        Convert PDF pages to PIL Image objects.
        """
        try:
            images = []
            with fitz.open(stream=pdf_data, filetype="pdf") as doc:
                for page_num in range(len(doc)):
                    page = doc.load_page(page_num)
                    # Render page to image with higher resolution for better OCR
                    mat = fitz.Matrix(2.0, 2.0)  # 2x zoom for better quality
                    pix = page.get_pixmap(matrix=mat)

                    # Convert to PIL Image
                    img_data = pix.tobytes("png")
                    images.append(img_data)

            return images
        except Exception as e:
            logger.error(f"Error converting PDF to images: {e}")
            return []

    def segments_to_embeddings(self, segments: Iterable[Segment]) -> List[bytes]:
        """
        Convert segments to embeddings using the embedder.
        """
        texts = [segment.text for segment in segments]
        vectors = self.embedder.embed_texts(texts)
        return [v.tobytes() for v in vectors]


    def response_handler(self, query, blobs, response, r_blobs):

        try:
            uniqueids = [e["_uniqueid"]
                         for e in response[0]["FindBlob"]["entities"]]
            urls = [e.get("url")
                    for e in response[0]["FindBlob"]["entities"]]
        except:
            logger.exception(f"error: {response}")
            return 0

        total_segments = 0
        for uniqueid, url, b in zip(uniqueids, urls, r_blobs):
            if self.has_text_content(b):
                logger.info("PDF has extractable text, skipping OCR processing")
                continue

            logger.debug(f"Processing uniqueid {uniqueid} {url=}")
            blob_segments = 0
            logger.info(f"Processing uniqueid {uniqueid} {url=}")
            text_blocks = []
            block_ids = []

            pdf_ref = 1
            block_query = []
            block_query.append(
                { 
                    "FindBlob": {
                        "constraints": {
                            "_uniqueid": ["==", uniqueid]
                        },
                        "_ref": pdf_ref,
                    }
                }
            )

            for page, image in enumerate(self.pdf_to_images(b), start=1):
                logger.info(f"Processing page {page}")
                text = self.ocr.bytes_to_text(image)
                if text:
                    logger.info(f"Extracted text from page {page}: {text[:100]}...")
                else:
                    logger.warning(f"No text extracted from page {page}")
                    continue
                block_id = str(uuid.uuid4())
                properties = {
                    "text": text,
                    "page_number": page,
                    "source_type": "pdf",
                    "ocr_method": self.ocr.method,
                    "id": block_id,
                }
                block_query.append(
                    {
                        "AddEntity": {
                            "class": "ExtractedText",
                            "properties": properties,
                            "connect": {
                                "ref": pdf_ref,
                                "class": "pdfHasTextBlock",
                            },
                        }
                    }
                )
                block = TextBlock(text=text, page_number=page)
                text_blocks.append(block)
                block_ids.append(block_id)

            status, r, _ = self.pool.execute_query(block_query)
            assert status == 0, f"Query failed: {r}"

            if self.extract_embeddings:
                for text_block, block_id in zip(text_blocks, block_ids):
                    segments = self.segmenter.segment([text_block])
                    segment_number = 1

                    for segment_batch in batch(segments, 100):
                        desc_query = []
                        desc_blobs = []
                        blob_ref = 1
                        text_ref = 2

                        # Find the blob
                        desc_query.append({
                            "FindBlob": {
                                "_ref": blob_ref,
                                "constraints": {
                                    "_uniqueid": ["==", uniqueid]
                                },
                            }
                        })
                        # Find the text block
                        desc_query.append({
                            "FindEntity": {
                                "_ref": text_ref,
                                "constraints": {
                                    "id": ["==", block_id]
                                },
                            }
                        })

                        embeddings = self.segments_to_embeddings(segment_batch)

                        for embedding, segment in zip(embeddings, segment_batch):
                            blob_segments += 1
                            descriptor_ref = len(desc_query) + 1
                            properties = {}
                            if segment.title:
                                properties["title"] = segment.title
                            if segment.text:
                                properties["text"] = segment.text
                            properties["type"] = "text"
                            properties["source_type"] = "pdf"
                            properties["total_tokens"] = segment.total_tokens
                            properties["segment_number"] = segment_number
                            segment_number += 1
                            properties["extraction_type"] = "ocr"
                            properties["ocr_method"] = self.ocr.method

                            properties["page_number"] = page

                            if url is not None:
                                segment_url = segment.url(url)
                                properties["url"] = segment_url

                            desc_query.append({
                                "AddDescriptor": {
                                    "set": self.embedder.descriptor_set,
                                    "connect": {
                                        "ref": text_ref,
                                        "class": "extractedTextHasDescriptor",
                                    },
                                    "properties": properties,
                                    "_ref": descriptor_ref,
                                }
                            })
                            desc_query.append({
                                "AddConnection": {
                                    "class": "pdfHasDescriptor",
                                    "src": blob_ref,
                                    "dst": descriptor_ref,
                                }
                            })
                            desc_blobs.append(embedding)

                        status, r, _ = self.pool.execute_query(desc_query, desc_blobs)
                        assert status == 0, f"Query failed: {r}"
                        logger.debug(f"Added {len(segment_batch)} segments")
                logger.debug(
                    f"Processed {blob_segments} segments for uniqueid {uniqueid}.")

                total_segments += blob_segments

        done_query = []
        done_query.append({
            "FindBlob": {
                "_ref": 1,
                "constraints": {
                    "_uniqueid": ["in", uniqueids]
                },
            }
        })
        done_query.append({
            "UpdateBlob": {
                "ref": 1,
                "properties": {
                    self.done_property: True,
                },
            }
        })

        status, r, _ = self.pool.execute_query(done_query)
        assert status == 0, f"Query failed: {r}"

        logger.info(f"Total segments processed: {total_segments}")
