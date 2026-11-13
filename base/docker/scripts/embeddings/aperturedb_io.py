"""Embedding search spaces are descriptor classes with native vector indexes."""
from typing import Optional
import logging

from aperturedb.CommonLibrary import execute_query
from aperturedb.Connector import Connector
from aperturedb.DescriptorEngines import engines_object
from aperturedb.Utils import Utils

logger = logging.getLogger(__name__)


def _execute(client, query):
    status, response, _ = execute_query(client, query)
    if status != 0:
        raise RuntimeError(f"Vector index query failed: {response}")
    return response


def find_descriptor_set(client: Connector, descriptor_set: str) -> Optional[dict]:
    """Return embedding metadata from a descriptor class's vector index."""
    response = _execute(client, [{"GetIndexes": {
        "target": "descriptor", "class": descriptor_set,
        "property": "_blob", "kind": "vector"}}])
    indexes = response[0]["GetIndexes"].get("indexes", [])
    if not indexes:
        return None
    if len(indexes) != 1:
        raise ValueError(f"Expected one vector index for {descriptor_set}: {indexes}")
    metadata = dict(indexes[0].get("metadata") or {})
    for name in ("embeddings_provider", "embeddings_model", "embeddings_pretrained",
                 "embeddings_fingerprint"):
        metadata.setdefault(name, None)
    return metadata


def add_descriptor_set(client: Connector, descriptor_set: str, metric: str,
                       dimensions: int, engine: str,
                       properties: Optional[dict] = None) -> None:
    """Create a vector index, keeping customer model metadata separate."""
    _execute(client, [{"CreateIndex": {
        "target": "descriptor", "class": descriptor_set,
        "property": "_blob", "kind": "vector",
        "params": {"dimensions": dimensions, "metric": metric,
                   "engines": engines_object(engine)},
        "metadata": properties or {}}}])


def update_descriptor_set(client: Connector, descriptor_set: str,
                          properties: dict) -> None:
    """Merge metadata without dropping fingerprint/model fields."""
    metadata = find_descriptor_set(client, descriptor_set)
    if metadata is None:
        raise ValueError(f"No vector index for {descriptor_set}")
    metadata.update(properties)
    metadata = {key: value for key, value in metadata.items() if value is not None}
    _execute(client, [{"UpdateIndex": {
        "target": "descriptor", "class": descriptor_set,
        "property": "_blob", "kind": "vector", "metadata": metadata}}])


def delete_descriptor_set(client: Connector, descriptor_set: str) -> None:
    """Remove descriptors, indexes and dependent connection categories atomically."""
    if not Utils(client).remove_descriptorset(descriptor_set):
        raise RuntimeError(f"Could not remove descriptor class {descriptor_set}")
