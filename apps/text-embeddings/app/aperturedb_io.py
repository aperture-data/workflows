from schema import Segment, Embedding
from typing import Iterator, Optional, Tuple
from uuid import uuid4
import math
import json
from datetime import datetime, timezone
from aperturedb.CommonLibrary import create_connector, execute_query
import logging
from batcher import SymbolicBatcher

logger = logging.getLogger(__name__)

INPUT_SPEC_CLASS = "SegmentationSpec"
SPEC_CLASS = "EmbeddingsSpec"
RUN_CLASS = "EmbeddingsRun"


class AperturedbIO:
    """Class to handle interactions with ApertureDB for embeddings jobs.
    This class is specific to the task and insulates other components from
    the details of the database."""

    def __init__(self,
                 input_spec_id: str,
                 spec_id: str,
                 run_id: str,
                 descriptorset_name: str,
                 engine: str,
                 embedder: "BatchEmbedder",
                 batch_size: int = 100):
        self.input_spec_id = input_spec_id
        self.spec_id = spec_id
        self.run_id = run_id
        self.descriptorset_name = descriptorset_name
        self.engine = engine
        self.embedder = embedder
        self.batch_size = batch_size
        self.db = create_connector()
        self.start_time = datetime.now(timezone.utc)
        self.batcher = SymbolicBatcher(
            execute_query=self.execute_query,
            batch_size=batch_size,
            prolog=self._batcher_prolog)
        self.n_embeddings = 0

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
        """Flush any remaining descriptors and update job document"""
        if exc_type is not None:
            logger.error(f"Error during processing: {exc_value}")
        self.batcher.flush()
        self.create_run()

    def _batcher_prolog(self) -> list[dict]:
        """Prolog function to create a new batch"""
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
        ]

    def _filter_null_properties(self, obj: dict) -> dict:
        """Filter out null properties from a dictionary"""
        return {k: v for k, v in obj.items() if v is not None}

    def create_spec(self) -> None:
        """Create spec document in ApertureDB"""
        logging.info(f"Starting {SPEC_CLASS} at {self.start_time.isoformat()}")

        self.execute_query([
            {
                "FindEntity": {
                    "with_class": INPUT_SPEC_CLASS,
                    "constraints": {
                        "id": ["==", self.input_spec_id],
                    },
                    "_ref": 1,
                }
            },
            {
                "AddEntity": {
                    "class":  SPEC_CLASS,
                    "properties": {
                        "segmentation_spec_id": self.input_spec_id,
                        "id": self.spec_id,
                        "model": self.embedder.model_spec,
                        "model_fingerprint": self.embedder.fingerprint_hash(),
                        "dimensions": self.embedder.dimensions(),
                        "metric": self.embedder.metric(),
                        "descriptorset_name": self.descriptorset_name,
                        "engine": self.engine,
                    },
                    "connect": {
                        "ref": 1,
                        "class": "segmentationSpecHasEmbeddingsSpec",
                        "direction": "in",
                    }
                }
            }])

    def create_descriptorset(self) -> None:
        """Finds or creates descriptor set in ApertureDB"""
        response, _ = self.execute_query([
            {
                "FindEntity": {
                    "with_class": SPEC_CLASS,
                    "constraints": {
                        "id": ["==", self.spec_id],
                    },
                    "_ref": 1,
                }
            },
            {
                "FindDescriptorSet": {
                    "with_name": self.descriptorset_name,
                    "metrics": True,
                    "results": {
                        "list": ["model", "model_fingerprint"],
                    },
                    "_ref": 2,
                }
            },
            {
                "AddConnection": {
                    "src": 1,
                    "dst": 2,
                    "class": "embeddingsSpecHasDescriptorSet",
                }
            },
        ])

        logger.info(
            f"Preparing to create descriptor set {self.descriptorset_name} with model {self.embedder.model_spec}, fingerprint {self.embedder.fingerprint_hash()}, metric {self.embedder.metric()}, dimensions {self.embedder.dimensions()}")

        if "entities" not in response[1]["FindDescriptorSet"]:
            logger.info(
                f"Creating new descriptor set {self.descriptorset_name}")
            self.execute_query([
                {
                    "FindEntity": {
                        "with_class": SPEC_CLASS,
                        "constraints": {
                            "id": ["==", self.spec_id],
                        },
                        "_ref": 1,
                    }
                },
                {
                    "AddDescriptorSet": {
                        "name": self.descriptorset_name,
                        "engine": self.engine,
                        "properties": {
                            "model": self.embedder.model_spec,
                            "model_fingerprint": self.embedder.fingerprint_hash(),
                        },
                        "metric": self.embedder.metric(),
                        "dimensions": self.embedder.dimensions(),
                        "connect": {
                            "ref": 1,
                            "class": "embeddingsSpecHasDescriptorSet",
                            "direction": "in",
                        },
                    }
                }
            ])
        else:
            logger.info(
                f"Descriptor set {self.descriptorset_name} already exists")
            e = response[1]["FindDescriptorSet"]["entities"][0]
            if e["model"] != self.embedder.model_spec:
                raise ValueError(
                    f"Descriptor set {self.descriptorset_name} already exists with different model {e['model']}, wanted to set {self.embedder.model_spec}")
            fingerprint_hash = self.embedder.fingerprint_hash()
            if e["model_fingerprint"] != fingerprint_hash:
                # Don't raise an error, just log it
                logger.error(
                    f"Descriptor set {self.descriptorset_name} already exists with different fingerprint found {e['model_fingerprint']}, expected {fingerprint_hash}")

            if self.embedder.metric() not in e["_metrics"] != self.embedder.metric():
                raise ValueError(
                    f"Descriptor set {self.descriptorset_name} already exists with different metric {e['_metrics']}")

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
                        "class": "_Descriptor",
                        "property_key": "spec_id",
                    }
                },
                {
                    "CreateIndex": {
                        "index_type": "entity",
                        "class": "_Descriptor",
                        "property_key": "run_id",
                    }
                },
            ],
            success_statuses=[0, 2],  # 0 = success, 2 = already exists
        )

    def create_run(self) -> None:
        """Create and connect EmbeddingsRun document in ApertureDB"""
        logger.info(f"Ending run {self.run_id}")
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
                        "n_embeddings": self.n_embeddings,
                    },
                    "connect": {
                        "ref": 1,
                        "class": "embeddingsSpecHasRun",
                        "direction": "in",
                    },
                    "_ref": 2,
                },
            },
            {
                "FindDescriptor": {
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
                    "class": "embeddingsRunHasDescriptor",
                }
            },
        ])

    def get_segments(self) -> Iterator[Segment]:
        """Retrieve text segments from ApertureDB"""
        query = [
            {
                "FindEntity": {
                    "with_class": INPUT_SPEC_CLASS,
                    "constraints": {
                        "id": ["==", self.input_spec_id],
                    },
                    "_ref": 1,
                }
            },
            {
                "FindEntity": {
                    "with_class": "Segment",
                    "is_connected_to": {
                        "ref": 1,
                        "connection_class": "segmentationSpecHasSegment",
                        "direction": "out",
                    },
                    "results": {
                        "list": ["id", "url", "text", "title"],
                    },
                    "batch": {},
                }
            },
        ]

        results, _ = self.execute_query(query)
        n_results = results[1]["FindEntity"]["batch"]["total_elements"]
        assert n_results > 0, f"No text segments found for input {self.input_spec_id}"
        query[1]["FindEntity"]["batch"]["batch_size"] = self.batch_size
        n_batches = int(math.ceil(n_results / self.batch_size))

        for batch_id in range(n_batches):
            query[1]["FindEntity"]["batch"]["batch_id"] = batch_id
            results, _ = self.execute_query(query)
            entities = results[1]["FindEntity"]["entities"]

            for result in entities:
                yield Segment(
                    id=result["id"],
                    url=result["url"],
                    text=result["text"],
                    title=result.get("title", None),
                )

    def create_embedding(self, embedding: Embedding) -> None:
        """Create a Descriptor in ApertureDB, linked to the EmbeddingsSpec and Segment"""
        self.n_embeddings += 1
        self.batcher.add([
            {
                "FindEntity": {
                    "with_class": "Segment",
                    "constraints": {
                        "id": ["==", embedding.segment_id],
                    },
                    "_ref": "SEGMENT",
                }
            },
            {
                "AddDescriptor": {
                    "set": self.descriptorset_name,
                    "properties": {
                        "segment_id": embedding.segment_id,
                        "uniqueid": str(uuid4()),  # LangChain supported field
                        "spec_id": self.spec_id,
                        "run_id": self.run_id,
                        "text": embedding.text,  # LangChain supported field
                        "url": embedding.url,
                        "lc_url": embedding.url,  # LangChain supported field
                        "title": embedding.title,
                    },
                    "connect": {
                        "ref": "SEGMENT",
                        "class": "segmentHasDescriptor",
                        "direction": "in",
                    },
                    "_ref": "TEMP",
                }
            },
            {
                "AddConnection": {
                    "src": "SPEC",
                    "dst": "TEMP",
                    "class": "embeddingsSpecHasDescriptor",
                }
            }
        ], [embedding.vector.tobytes()])

    def delete_spec(self, spec_id) -> None:
        """Delete an EmbeddingsSpec document and all its dependent artefacts"""
        logger.info(f"Deleting {SPEC_CLASS} {spec_id}")
        response, _ = self.execute_query([
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
                "FindDescriptorSet": {
                    "is_connected_to": {
                        "ref": 1,
                        "connection_class": "embeddingsSpecHasDescriptorSet",
                    },
                    "counts": True,
                    "uniqueids": True,
                    "_ref": 2,
                },
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
                    "_ref": 3,
                }
            },
            {
                "DeleteEntity": {
                    "ref": 3,
                }
            },
            {
                "FindDescriptor": {
                    "constraints": {
                        "spec_id": ["==", spec_id],
                    },
                    "_ref": 4,
                }
            },
            {
                "DeleteDescriptor": {
                    "ref": 4,
                }
            },
        ])

        # It is possible for multiple EmbeddingsSpec documents to share the same DesceriptorSet
        # If the descriptor set is empty, delete it
        descriptorset_count = response[1]["FindDescriptorSet"].get("count", 0)
        deleted_count = response[6]["DeleteDescriptor"]["count"]
        if descriptorset_count > 0 and descriptorset_count == deleted_count:
            descriptorset_id = response[1]["FindDescriptorSet"]["entities"][0]["id"]
            logger.info("Deleting empty descriptor set")
            self.execute_query([
                {
                    "FindDescriptorSet": {
                        "constraints": {
                            "_uniqueid": ["==", descriptorset_id],
                        },
                        "_ref": 1,
                    }
                },
                {
                    "DeleteEntity": {
                        "ref": 1,
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
        assert self.does_entity_exist(INPUT_SPEC_CLASS, self.input_spec_id), \
            f"{INPUT_SPEC_CLASS} {self.input_spec_id} does not exist"

    def ensure_output_does_not_exist(self):
        assert not self.does_entity_exist(SPEC_CLASS, self.spec_id), \
            f"{SPEC_CLASS} {self.spec_id} already exists"
