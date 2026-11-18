from aperturedb.Constraints import predicate
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
        self._record_run = False

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
        if not self.db.last_query_ok():
            raise ValueError(
                f"Query failed with status {status}: {json.dumps(results, indent=2)}")

        return results, result_blobs

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Flush any remaining descriptors and update job document"""
        if exc_type is not None:
            logger.error(f"Error during processing: {exc_value}")
        self.batcher.flush()
        if self._record_run:
            self.create_run()

    def _batcher_prolog(self) -> list[dict]:
        """Prolog function to create a new batch"""
        return [
            {
                "FindEntity": {
                    "with_class": SPEC_CLASS,
                    "constraints": ["$id", "==", {"json": self.spec_id}],
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
                    "constraints": ["$id", "==", {"json": self.input_spec_id}],
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
                        "dimensions": self.embedder.dimensions,
                        "metric": self.embedder.metric,
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
        self._record_run = True

    def connect_descriptor_set(self) -> None:
        """Verify the descriptor class has its native vector index."""
        response, _ = self.execute_query([{"GetIndexes": {"target": "descriptor",
                                                          "class": self.descriptorset_name,
                                                          "property": "_blob",
                                                          "kind": self.engine.lower()}}])
        indexes = response[0]["GetIndexes"].get("indexes", [])
        if len(indexes) != 1:
            raise ValueError(
                f"Expected one vector index for descriptor class {self.descriptorset_name}; "
                f"found {len(indexes)}")

    def create_indexes(self) -> None:
        """Create indexes"""
        logger.info(
            f"Creating indexes; may cause partial errors if they already exist")
        self.execute_query(
            [
                {
                    "CreateIndex": {
                        "target": "entity",
                        "kind": "ordered",
                        "class": SPEC_CLASS,
                        "property": "id",
                    }
                },
                {
                    "CreateIndex": {
                        "target": "entity",
                        "kind": "ordered",
                        "class": RUN_CLASS,
                        "property": "id",
                    }
                },
                {
                    "CreateIndex": {
                        "target": "entity",
                        "kind": "ordered",
                        "class": RUN_CLASS,
                        "property": "spec_id",
                    }
                },
                {
                    "CreateIndex": {
                        "target": "descriptor",
                        "kind": "ordered",
                        "class": self.descriptorset_name,
                        "property": "spec_id",
                    }
                },
                {
                    "CreateIndex": {
                        "target": "descriptor",
                        "kind": "ordered",
                        "class": self.descriptorset_name,
                        "property": "run_id",
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
                    "constraints": ["$id", "==", {"json": self.spec_id}],
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
                    "with_class": self.descriptorset_name,
                    "constraints": ["$run_id", "==", {"json": self.run_id}],
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
        """Read a fixed segmentation result with ordinary offset/limit pages."""
        offset = 0
        while True:
            results, _ = self.execute_query([
                {"FindEntity": {"with_class": INPUT_SPEC_CLASS,
                                "constraints": predicate("id", "==", self.input_spec_id), "_ref": 1}},
                {"FindEntity": {"with_class": "Segment", "is_connected_to": {
                    "ref": 1, "connection_class": "segmentationSpecHasSegment", "direction": "out"},
                    "offset": offset, "limit": self.batch_size, "sort": "_uniqueid",
                    "results": {"list": ["id", "url", "text", "title"]}}}])
            entities = results[1]["FindEntity"].get("entities", [])
            for result in entities:
                yield Segment(id=result["id"], url=result["url"], text=result["text"], title=result.get("title"))
            if len(entities) < self.batch_size:
                return
            offset += len(entities)

    def create_embedding(self, embedding: Embedding) -> None:
        """Create a Descriptor in ApertureDB, linked to the EmbeddingsSpec and Segment"""
        self.n_embeddings += 1
        self.batcher.add([{"FindEntity": {"with_class": "Segment",
                                          "constraints": ["$id",
                                                          "==",
                                                          {"json": embedding.segment_id}],
                                          "_ref": "SEGMENT",
                                          }},
                          {'AddDescriptor': {'class': self.descriptorset_name,
                                             'properties': self._filter_null_properties({'segment_id': embedding.segment_id,
                                                                                         'uniqueid': str(uuid4()),
                                                                                         'spec_id': self.spec_id,
                                                                                         'run_id': self.run_id,
                                                                                         'text': embedding.text,
                                                                                         'url': embedding.url,
                                                                                         'lc_url': embedding.url,
                                                                                         'title': embedding.title}),
                                             'connect': {'ref': 'SEGMENT',
                                                         'class': 'segmentHasDescriptor',
                                                         'direction': 'in'},
                                             '_ref': 'TEMP'}},
                          {"AddConnection": {"src": "SPEC",
                                             "dst": "TEMP",
                                             "class": "embeddingsSpecHasDescriptor",
                                             }}],
                         [embedding.vector.tobytes()])

    def delete_spec(self, spec_id) -> None:
        """Remove a job's descriptors and runs, then remove its empty vector class."""
        response, _ = self.execute_query([{"FindEntity": {"with_class": SPEC_CLASS, "constraints": predicate(
            "id", "==", spec_id), "results": {"list": ["descriptorset_name"]}}}])
        specs = response[0]["FindEntity"].get("entities", [])
        if not specs:
            return
        descriptor_class = specs[0]["descriptorset_name"]
        self.execute_query([
            {"DeleteDescriptor": {"with_class": descriptor_class,
                                  "constraints": predicate("spec_id", "==", spec_id)}},
            {"DeleteEntity": {"with_class": RUN_CLASS,
                              "constraints": predicate("spec_id", "==", spec_id)}},
            {"DeleteEntity": {"with_class": SPEC_CLASS, "constraints": predicate("id", "==", spec_id)}}])
        from aperturedb.Utils import Utils
        utils = Utils(self.db)
        if utils.count_descriptors_in_set(descriptor_class) == 0:
            if not utils.remove_descriptorset(descriptor_class):
                raise RuntimeError(f"Could not remove empty descriptor class {descriptor_class}")

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
                    "constraints": ["$id", "==", {"json": id_}],
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
