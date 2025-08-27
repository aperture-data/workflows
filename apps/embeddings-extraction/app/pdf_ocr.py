import math
import logging
from itertools import islice
from typing import Iterable, Tuple, List, Optional

import torch
import numpy as np
import clip
import cv2
from PIL import Image
import pytesseract
from io import BytesIO
import fitz  # PyMuPDF

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

    def pdf_to_images(self, pdf_data: bytes) -> List[Image.Image]:
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
                    img = Image.open(BytesIO(img_data)).convert("RGB")
                    images.append(img)
            
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

    def blob_to_embeddings(self, blob: bytes) -> Iterable[Tuple[Segment, bytes]]:
        """
        Process PDF blob: check for text, convert to images if needed, 
        extract text via OCR, segment, and generate embeddings.
        """
        # First check if PDF has extractable text
        if self.has_text_content(blob):
            logger.info("PDF has extractable text, skipping OCR processing")
            return

        # Convert PDF to images
        images = self.pdf_to_images(blob)
        if not images:
            logger.warning("No images extracted from PDF")
            return

        logger.info(f"Extracted {len(images)} pages from PDF")

        # Extract text from each image
        all_text_blocks = []
        for page_num, image in enumerate(images, start=1):
            text = self.ocr.image_to_text(image)
            if text:
                # Create text block with page number
                block = TextBlock(text=text, page_number=page_num)
                all_text_blocks.append(block)
                logger.debug(f"Extracted text from page {page_num}: {text[:100]}...")

        if not all_text_blocks:
            logger.warning("No text extracted from PDF images")
            return

        logger.info(f"Extracted {len(all_text_blocks)} text blocks from PDF")

        # Segment the text blocks
        segments = self.segmenter.segment(all_text_blocks, clean_only=False)

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
            uniqueids = [e["_uniqueid"]
                         for e in response[0]["FindBlob"]["entities"]]
            urls = [e.get("url")
                    for e in response[0]["FindBlob"]["entities"]]
        except:
            logger.exception(f"error: {response}")
            return 0

        total_segments = 0
        for uniqueid, url, b in zip(uniqueids, urls, r_blobs):
            blob_segments = 0
            segments_and_embeddings = self.blob_to_embeddings(b)
            
            # If no segments (PDF had text content), mark as processed and continue
            if not segments_and_embeddings:
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
                            "segments": 0,
                        },
                    }
                })
                self.pool.execute_query(query)
                continue
            
            for segment_batch in batch(segments_and_embeddings, 100):
                query = []
                vectors = []
                blob_ref = 1
                text_ref = 2

                query.append({
                    "FindBlob": {
                        "_ref": blob_ref,
                        "constraints": {
                            "_uniqueid": ["==", uniqueid]
                        },
                    }
                })

                query.append({
                    "AddEntity": {
                        "class": "PDFExtractedText",
                        "properties": {
                            "text": "test",
                            "ocr_method": self.ocr.method,
                            "source_type": "pdf",
                        },
                        "connect": {
                            "ref": blob_ref,
                            "class": "pdfHasExtractedText",
                        },
                        "_ref": text_ref,
                    }
                })

                for segment, embedding, number in segment_batch:
                    blob_segments += 1
                    descriptor_ref = len(query) + 1
                    properties = {}
                    if segment.title:
                        properties["title"] = segment.title
                    if segment.text:
                        properties["text"] = segment.text
                    properties["type"] = "text"
                    properties["source_type"] = "pdf"
                    properties["total_tokens"] = segment.total_tokens
                    properties["segment_number"] = number
                    properties["extraction_type"] = "ocr"
                    properties["ocr_method"] = self.ocr.method

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
                                "ref": text_ref,
                                "class": "extractedTextHasDescriptor",
                            },
                            "properties": properties,
                            "_ref": descriptor_ref,
                        }
                    })
                    query.append({
                        "AddConnection": {
                            "class": "pdfHasDescriptor",
                            "src": blob_ref,                            
                            "dst": descriptor_ref,
                        }
                    })
                    vectors.append(embedding)

                self.pool.execute_query(query, vectors)

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
            self.pool.execute_query(query)

            logger.debug(
                f"Processed {blob_segments} segments for uniqueid {uniqueid}.")

        logger.info(f"Total segments processed: {total_segments}")
