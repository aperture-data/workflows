from text_extraction.schema import CrawlDocument, Segment, ImageBlock, FullTextBlock
from typing import Iterator, Optional, Tuple
from uuid import uuid4
import math
import json
from datetime import datetime, timezone
from aperturedb.CommonLibrary import create_connector, execute_query
import logging
from batcher import SymbolicBatcher

logger = logging.getLogger(__name__)

INPUT_SPEC_CLASS = "CrawlSpec"
SPEC_CLASS = "SegmentationSpec"
RUN_CLASS = "SegmentationRun"


class AperturedbIO:
    """Class to handle interactions with ApertureDB for segmentation jobs.
    This class is specific to the task and insulates other components from
    the details of the database."""

    def __init__(self, crawl_spec_id: str, spec_id: str, run_id: str, batch_size: int = 100):
        self.crawl_spec_id = crawl_spec_id
        self.spec_id = spec_id
        self.run_id = run_id
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
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Flush any remaining segments and update job document"""
        if exc_type is not None:
            logger.error(f"Error during processing: {exc_value}")
        self.batcher.flush()
        self.create_run()

    def _batcher_prolog(self) -> list[dict]:
        """Prolog function to create a new batch"""
        assert self.current_document is not None, "No current document set"
        return [
            {
                "FindEntity": {
                    "with_class": SPEC_CLASS,
                    "constraints": {
                        "id": ["==", self.spec_id],
                    },
                    "_ref": "SPEC",
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

    def create_spec(self) -> None:
        """Create SegmentationSpec document in ApertureDB"""
        logging.info(f"Starting {SPEC_CLASS} at {self.start_time.isoformat()}")
        self.execute_query([
            {
                "FindEntity": {
                    "with_class": INPUT_SPEC_CLASS,
                    "constraints": {
                        "id": ["==", self.crawl_spec_id],
                    },
                    "_ref": 1,
                }
            },
            {
                "AddEntity": {
                    "class":  SPEC_CLASS,
                    "properties": {
                        "crawl_spec_id": self.crawl_spec_id,
                        "id": self.spec_id,
                    },
                    "connect": {
                        "ref": 1,
                        "class": "crawlSpecHasSegmentationSpec",
                        "direction": "in",
                    }
                }
            }])

    def create_indexes(self) -> None:
        """Create indexes"""
        logger.info(
            f"Creating indexes; may cause partial errors if they already exist")
        self.execute_query(
            [
                {
                    "CreateIndex": {
                        "index_type": "entity",
                        "class": SPEC_CLASS,
                        "property_key": "id",
                    }
                },
                {
                    "CreateIndex": {
                        "index_type": "entity",
                        "class": RUN_CLASS,
                        "property_key": "id",
                    }
                },
                {
                    "CreateIndex": {
                        "index_type": "entity",
                        "class": RUN_CLASS,
                        "property_key": "spec_id",
                    }
                },
                {
                    "CreateIndex": {
                        "index_type": "entity",
                        "class": "Segment",
                        "property_key": "id",
                    }
                },
                {
                    "CreateIndex": {
                        "index_type": "entity",
                        "class": "Segment",
                        "property_key": "spec_id",
                    }
                },
                {
                    "CreateIndex": {
                        "index_type": "entity",
                        "class": "Segment",
                        "property_key": "run_id",
                    }
                },
            ],
            success_statuses=[0, 2],  # 0 = success, 2 = already exists
        )

    def create_run(self) -> None:
        """Create and connect SegmentationRun document in ApertureDB"""
        logger.info(f"Ending run {self.run_id} with "
                    f"{self.n_segments} segments, {self.n_images} images, {self.n_full_texts} texts")
        end_time = datetime.now(timezone.utc)
        duration = (end_time - self.start_time).total_seconds()
        logger.info(f"Duration: {duration} seconds")

        self.execute_query([
            {
                "FindEntity": {
                    "with_class": SPEC_CLASS,
                    "constraints": {
                        "id": ["==", self.spec_id],
                    },
                    "_ref": 1,
                },
            },
            {
                "AddEntity": {
                    "class": RUN_CLASS,
                    "properties": {
                        "spec_id": self.spec_id,
                        "id": self.run_id,
                        "end_time": {"_date": end_time.isoformat()},
                        "duration": duration,
                        "n_segments": self.n_segments,
                        "n_images": self.n_images,
                        "n_full_texts": self.n_full_texts,
                    },
                    "connect": {
                        "ref": 1,
                        "class": "segmentationSpecHasRun",
                        "direction": "in",
                    },
                    "_ref": 2,
                },
            },
            {
                "FindEntity": {
                    "with_class": "Segment",
                    "constraints": {
                        "run_id": ["==", self.run_id],
                    },
                    "_ref": 3,
                },
            },
            {
                "AddConnection": {
                    "src": 2,
                    "dst": 3,
                    "class": "segmentationRunHasSegment",
                }
            },
            {
                "FindEntity": {
                    "with_class": "ImageText",
                    "constraints": {
                        "run_id": ["==", self.run_id],
                    },
                    "_ref": 4,
                },
            },
            {
                "AddConnection": {
                    "src": 2,
                    "dst": 4,
                    "class": "segmentationRunHasImageText",
                }
            },
            {
                "FindEntity": {
                    "with_class": "FullTextBlock",
                    "constraints": {
                        "run_id": ["==", self.run_id],
                    },
                    "_ref": 5,
                },
            },
            {
                "AddConnection": {
                    "src": 2,
                    "dst": 5,
                    "class": "segmentationRunHasImageText",
                }
            },
        ])

    def get_crawl_documents(self) -> Iterator[CrawlDocument]:
        """Retrieve crawled documents from ApertureDB that have not been segmented"""
        query = [
            {
                "FindEntity": {
                    "with_class": INPUT_SPEC_CLASS,
                    "constraints": {
                        "id": ["==", self.crawl_spec_id],
                    },
                    "_ref": 1,
                }
            },
            {
                "FindEntity": {
                    "with_class": "CrawlDocument",
                    "is_connected_to": {
                        "ref": 1,
                        "connection_class": "crawlSpecHasDocument",
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
        """Create a Segment document in ApertureDB, linked to the CrawlDocument and SegmentationSpec"""
        assert self.current_document is not None, "No current document set"
        self.n_segments += 1
        self.batcher.add([
            {
                "AddEntity": {
                    "class": "Segment",
                    "properties": {
                        "id": str(uuid4()),
                        "text": segment.text,
                        "kind": segment.kinds,
                        "url": segment.url(self.current_document.url),
                        "spec_id": self.spec_id,
                        "run_id": self.run_id,
                        "n_tokens": segment.total_tokens,
                        "n_characters": len(segment.text),
                        **({"title": segment.title} if segment.title else {}),
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
                    "src": "SPEC",
                    "dst": "TEMP",
                    "class": "segmentationSpecHasSegment",
                }
            }
        ])

    def create_image_block(self, block: ImageBlock) -> None:
        """Create a ImageBlock document in ApertureDB, linked to the CrawlDocument, SegmentationSpec, and possibly Image"""
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
                        "spec_id": self.spec_id,
                        "run_id": self.run_id,
                        **({"title": block.title} if block.title else {}),
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
                    "src": "SPEC",
                    "dst": "TEMP",
                    "class": "segmentationSpecHasImageText",
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

    def create_full_text_block(self, block: FullTextBlock) -> None:
        """Create a FullTextBlock document in ApertureDB, linked to the CrawlDocument and SegmentationSpec"""
        assert self.current_document is not None, "No current document set"
        self.n_full_texts += 1
        self.batcher.add([
            {
                "AddEntity": {
                    "class": "FullText",
                    "properties": {
                        "url": self.current_document.url,
                        "spec_id": self.spec_id,
                        "run_id": self.run_id,
                        **({"title": block.title} if block.title else {}),
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
                    "src": "SPEC",
                    "dst": "TEMP",
                    "class": "segmentationSpecHasFullText",
                }
            },
        ], [block.text.encode("utf-8")])

    def delete_spec(self, spec_id) -> None:
        """Delete a SegmentationSpec document and all its dependent artefacts"""
        logger.info(f"Deleting {SPEC_CLASS} {spec_id}")
        self.execute_query([
            {
                "FindEntity": {
                    "with_class": SPEC_CLASS,
                    "constraints": {
                        "id": ["==", spec_id],
                    },
                    "_ref": 1,
                }
            },
            {
                "DeleteEntity": {
                    "ref": 1,
                }
            },
            {
                "FindEntity": {
                    "with_class": RUN_CLASS,
                    "constraints": {
                        "spec_id": ["==", spec_id],
                    },
                    "_ref": 2,
                }
            },
            {
                "DeleteEntity": {
                    "ref": 2,
                }
            },
            {
                "FindEntity": {
                    "with_class": "Segment",
                    "constraints": {
                        "spec_id": ["==", spec_id],
                    },
                    "_ref": 3,
                }
            },
            {
                "DeleteEntity": {
                    "ref": 3,
                }
            },
            {
                "FindEntity": {
                    "with_class": "ImageText",
                    "constraints": {
                        "spec_id": ["==", spec_id],
                    },
                    "_ref": 4,
                }
            },
            {
                "DeleteEntity": {
                    "ref": 4,
                }
            },
            {
                "FindEntity": {
                    "with_class": "FullText",
                    "constraints": {
                        "spec_id": ["==", spec_id],
                    },
                    "_ref": 5,
                }
            },
            {
                "FindBlob": {
                    "is_connected_to": {
                        "ref": 5,
                        "connection_class": "fullTextHasBlob",
                    },
                    "_ref": 6,
                }
            },
            {
                "DeleteEntity": {
                    "ref": 5,
                }
            },
            {
                "DeleteBlob": {
                    "ref": 6,
                }
            }

        ])

    def delete_all(self) -> None:
        """Delete all SegmentationSpec documents and all their dependent artefacts
        """
        logger.info(f"Deleting all {SPEC_CLASS} documents")
        response, _ = self.execute_query([
            {
                "FindEntity": {
                    "with_class": SPEC_CLASS,
                    "results": {
                        "list": ["id"],
                    }
                }
            },
        ])

        if 'entities' not in response[0]["FindEntity"]:
            logger.info(f"No {SPEC_CLASS} documents found")
            return

        for entity in response[0]["FindEntity"]["entities"]:
            spec_id = entity["id"]
            logger.info(f"Deleting {SPEC_CLASS} {spec_id}")
            self.delete_spec(spec_id)

    def does_entity_exist(self, class_, id_) -> bool:
        """Check if an entity exists in ApertureDB"""
        query = [
            {
                "FindEntity": {
                    "with_class": class_,
                    "constraints": {
                        "id": ["==", id_],
                    },
                    "results": {"count": True},
                }
            },
        ]
        results, _ = self.execute_query(query)
        return results[0]["FindEntity"]["count"] > 0

    def ensure_input_exists(self):
        assert self.does_entity_exist(INPUT_SPEC_CLASS, self.crawl_spec_id), \
            f"Crawl{INPUT_SPEC_CLASS} {self.crawl_spec_id} does not exist"

    def ensure_output_does_not_exist(self):
        assert not self.does_entity_exist(SPEC_CLASS, self.spec_id), \
            f"{SPEC_CLASS} {self.spec_id} already exists"
