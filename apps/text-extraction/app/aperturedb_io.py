from schema import CrawlDocument, Segment, ImageBlock, FullTextBlock
from typing import Iterator, Optional, Tuple
from uuid import uuid4
import math
import json
from datetime import datetime, timezone
from aperturedb.CommonLibrary import create_connector, execute_query
import logging
from batcher import SymbolicBatcher

logger = logging.getLogger(__name__)

JOB_CLASS = "SegmentationJob"


class AperturedbIO:
    def __init__(self, crawl_id: str, batch_size: int = 100):
        self.crawl_id = crawl_id
        self.job_id = str(uuid4())
        self.batch_size = batch_size
        self.db = create_connector()
        self.start_time = datetime.now(timezone.utc)
        self.n_segments = 0
        self.n_images = 0
        self.n_full_texts = 0
        self.batcher = SymbolicBatcher(
            execute_query=self.execute_query,
            batch_size=batch_size,
            prolog=self._batcher_prolog)
        self.current_document = None

    def execute_query(self,
                      query: Iterator[dict],
                      blobs: Optional[Iterator[bytes]] = [],
                      success_statuses=[0],
                      strict_response_validation=True,
                      ) -> Tuple[list[dict], list[bytes]]:
        """Execute a query on ApertureDB and return the results

        TODO: Support mock
        """
        status, results, result_blobs = execute_query(
            client=self.db,
            query=query,
            blobs=blobs, strict_response_validation=strict_response_validation, success_statuses=success_statuses
        )
        return results, result_blobs

    def __enter__(self):
        self.create_job_document()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Flush any remaining segments and update job document"""
        if exc_type is not None:
            logger.error(f"Error during processing: {exc_value}")
        self.batcher.flush()
        self.update_job_document()

    def _batcher_prolog(self) -> list[dict]:
        """Prolog function to create a new batch"""
        assert self.current_document is not None, "No current document set"
        return [
            {
                "FindEntity": {
                    "with_class": JOB_CLASS,
                    "constraints": {
                        "id": ["==", self.job_id],
                    },
                    "_ref": "JOB",
                }
            },
            {
                "FindBlob": {
                    "constraints": {
                        "_uniqueid": ["==", self.current_document.document_id],
                    },
                    "_ref": "DOC",
                }
            },
        ]

    def _filter_null_properties(self, obj: dict) -> dict:
        """Filter out null properties from a dictionary"""
        return {k: v for k, v in obj.items() if v is not None}

    def create_job_document(self) -> None:
        """Create SegmentationJob document in ApertureDB"""
        logging.info(f"Starting {JOB_CLASS} at {self.start_time.isoformat()}")
        self.execute_query([
            {
                "FindEntity": {
                    "with_class": "Crawl",
                    "constraints": {
                        "id": ["==", self.crawl_id],
                    },
                    "_ref": 1,
                }
            },
            {
                "AddEntity": {
                    "class":  JOB_CLASS,
                    "properties": {
                        "start_time": {"_date": self.start_time.isoformat()},
                        "crawl_id": self.crawl_id,
                        "id": self.job_id,
                    },
                    "connect": {
                        "ref": 1,
                        "class": "crawlHasSegmentation",
                        "direction": "in",
                    }
                }
            }])

        self.execute_query(
            [
                {
                    "CreateIndex": {
                        "index_type": "entity",
                        "class": JOB_CLASS,
                        "property_key": "id",
                    }
                },
            ],
            success_statuses=[0, 2],  # 0 = success, 2 = already exists
        )

    def update_job_document(self) -> None:
        """Find and update SegmentationJob document in ApertureDB"""
        logger.info(f"Ending {JOB_CLASS} {self.job_id} with "
                    f"{self.n_segments} segments, {self.n_images} images, {self.n_full_texts} texts")
        end_time = datetime.now(timezone.utc)
        duration = (end_time - self.start_time).total_seconds()
        logger.info(f"Duration: {duration} seconds")

        self.execute_query([
            {
                "FindEntity": {
                    "with_class": JOB_CLASS,
                    "constraints": {
                        "id": ["==", self.job_id],
                    },
                    "_ref": 1,
                },
            },
            {
                "UpdateEntity": {
                    "ref": 1,
                    "properties": {
                        "end_time": {"_date": end_time.isoformat()},
                        "duration": duration,
                        "n_segments": self.n_segments,
                        "n_images": self.n_images,
                        "n_full_texts": self.n_full_texts,
                    }
                },
            }
        ])

    def get_crawl_documents(self) -> Iterator[CrawlDocument]:
        """Retrieve crawled documents from ApertureDB that have not been segmented"""
        query = [
            {
                "FindEntity": {
                    "with_class": "Crawl",
                    "constraints": {
                        "id": ["==", self.crawl_id],
                    },
                    "_ref": 1,
                }
            },
            {
                "FindEntity": {
                    "with_class": "CrawlDocument",
                    "is_connected_to": {
                        "ref": 1,
                        "connection_class": "crawlHasDocument",
                        "direction": "out",
                    },
                    "uniqueids": True,
                    "results": {
                        "list": ["url", "content_type"],
                    },
                    "batch": {},
                }
            },
        ]

        results, _ = self.execute_query(query)
        n_results = results[1]["FindEntity"]["batch"]["total_elements"]
        assert n_results > 0, f"No documents found for crawl {self.crawl_id}"
        query[1]["FindEntity"]["batch"]["batch_size"] = self.batch_size
        n_batches = int(math.ceil(n_results / self.batch_size))

        for batch_id in range(n_batches):
            query[1]["FindEntity"]["batch"]["batch_id"] = batch_id
            results, _ = self.execute_query(query)
            entities = results[1]["FindEntity"]["entities"]
            entity_ids = [e["_uniqueid"] for e in entities]

            # Now fetch blobs (can't do this in the batch query)
            blob_results, blobs = self.execute_query([
                {
                    "FindEntity": {
                        "with_class": "CrawlDocument",
                        "constraints": {
                            "_uniqueid": ["in", entity_ids],
                        },
                        "_ref": 1,
                    }
                },
                {
                    "FindBlob": {
                        "is_connected_to": {
                            "ref": 1,
                            "connection_class": "crawlDocumentHasBlob",
                            "direction": "out",
                        },
                        "uniqueids": True,  # required to make group_by_source work
                        "blobs": True,
                        "group_by_source": True,
                    }
                },
            ])

            for result in entities:
                document_id = result["_uniqueid"]
                url = result["url"]
                content_type = result["content_type"]

                blob_entities = blob_results[1]["FindBlob"]["entities"][document_id]
                assert len(
                    blob_entities) == 1, f"Expected 1 blob for document {document_id}, found {len(blob_entities)}"
                blob = blobs[blob_entities[0]["_blob_index"]]
                yield CrawlDocument(
                    document_id=document_id,
                    url=url,
                    content_type=content_type,
                    blob=blob
                )

    def set_document(self, crawl_document: CrawlDocument) -> None:
        """Set the current document for batch processing"""
        self.current_document = crawl_document
        if not self.batcher.empty():
            self.batcher.add([
                {
                    "FindBlob": {
                        "constraints": {
                            "_uniqueid": ["==", self.current_document.document_id],
                        },
                        "_ref": "DOC",
                    }
                },
            ])

    def create_segment(self, segment: Segment) -> None:
        """Create a Segment document in ApertureDB, linked to the CrawlDocument and SegmentationJob"""
        assert self.current_document is not None, "No current document set"
        self.n_segments += 1
        self.batcher.add([
            {
                "AddEntity": {
                    "class": "Segment",
                    "properties": {
                        "text": segment.text,
                        "kind": segment.kinds,
                        "url": segment.url(self.current_document.url),
                    },
                    "connect": {
                        "ref": "DOC",
                        "class": "documentHasSegment",
                        "direction": "in",
                    },
                    "_ref": "TEMP",
                }
            },
            {
                "AddConnection": {
                    "src": "JOB",
                    "dst": "TEMP",
                    "class": "segmentationJobHasSegment",
                }
            }
        ])

    def create_image_block(self, block: ImageBlock):
        """Create a ImageBlock document in ApertureDB, linked to the CrawlDocument, SegmentationJob, and possibly Image"""
        assert self.current_document is not None, "No current document set"
        self.n_images += 1
        self.batcher.add([
            {
                "AddEntity": {
                    "class": "ImageText",
                    "properties": self._filter_null_properties({
                        "image_url": block.image_url,
                        "caption": block.caption,
                        "alt_text": block.alt_text,
                        "text": block.best_text,
                        "anchor": block.anchor,
                        "text_url": block.url(self.current_document.url),
                    }),
                    "connect": {
                        "ref": "DOC",
                        "class": "documentHasImageText",
                        "direction": "in",
                    },
                    "_ref": "TEMP",
                },
            },
            {
                "AddConnection": {
                    "src": "JOB",
                    "dst": "TEMP",
                    "class": "segmentationJobHasImageText",
                }
            },
            {
                "FindImage": {
                    "constraints": {
                        "url": ["==", block.image_url],
                    },
                    "_ref": "IMAGE",
                }
            },
            {
                "AddConnection": {
                    "src": "IMAGE",
                    "dst": "TEMP",
                    "class": "imageHasImageText",
                }
            }
        ])

    def create_full_text_block(self, block: FullTextBlock):
        """Create a FullTextBlock document in ApertureDB, linked to the CrawlDocument and SegmentationJob"""
        assert self.current_document is not None, "No current document set"
        self.n_full_texts += 1
        self.batcher.add([
            {
                "AddEntity": {
                    "class": "FullText",
                    "properties": {
                        "url": self.current_document.url,
                    },
                    "connect": {
                        "ref": "DOC",
                        "class": "documentHasFullText",
                        "direction": "in",
                    },
                    "_ref": "TEMP",
                },
            },
            {
                "AddBlob": {
                    "properties": {
                        "url": self.current_document.url,
                    },
                    "connect": {
                        "ref": "TEMP",
                        "class": "fullTextHasBlob",
                        "direction": "in",
                    },
                },
            },
            {
                "AddConnection": {
                    "src": "JOB",
                    "dst": "TEMP",
                    "class": "segmentationJobHasFullText",
                }
            },
        ], [block.text.encode("utf-8")])
