# This module populates the descriptor schema for ApertureDB.
# This schema contains a table for each descriptor class,
# and hence every AQL query includes `with_class`.
# These tables support find-similar queries.
#
# SELECT * FROM descriptor."crawl-to-rag"
# WHERE _find_similar = FIND_SIMILAR(text := 'find entity', k := 10)
# AND _blobs

from aperturedb.DescriptorEngines import ENGINE_ORDER
from aperturedb.ServerCompatibility import preferred_vector_index
from multicorn import TableDefinition, ColumnDefinition
from typing import List
from .common import Curry
from .column import property_columns, ColumnOptions, blob_columns, get_path_keys
from .aperturedb import get_classes
from .table import TableOptions, literal
import logging
import struct
import json
from datetime import datetime
from .aperturedb import execute_query
from .embedding import embed_texts, embed_images
import base64


logger = logging.getLogger(__name__)


def descriptor_set_supports_find_similar(properties: dict) -> bool:
    """
    Check if the descriptor class supports find-similar queries.
    This is determined by the presence of properties that allow embedding.
    """
    return all(properties.get(x)
               for x in ["embeddings_provider", "embeddings_model", "embeddings_pretrained"])


def descriptor_schema() -> List[TableDefinition]:
    """
    Return the descriptor class schema for ApertureDB.
    This is used to create the foreign tables for descriptors.
    Here we create a separate table for each descriptor class.
    """
    logger.info("Creating descriptor schema")
    results = []
    descriptor_sets = get_descriptor_sets()
    assert isinstance(descriptor_sets, dict), \
        f"Expected descriptor_sets to be a dict, got {type(descriptor_sets)}"
    for name, properties in descriptor_sets.items():
        table_name = name

        # We switch the "find similar" feature on if the descriptor class
        # has properties that allow us to find the correct embedding model.
        # Notionally we could allow direct vector queries regardless, but
        # this is a good heuristic to avoid unnecessary complexity.
        find_similar = True  # Raw vectors work without a model; text/image need metadata.

        columns = property_columns_for_descriptors_in_set(name)

        if find_similar:
            columns.append(ColumnDefinition(
                column_name="_find_similar",
                type_name="JSONB",
                options=ColumnOptions(
                    listable=False,
                    type="json",
                    modify_command_body=Curry(
                        find_similar_modify_command_body),
                    query_blobs=Curry(find_similar_query_blobs,
                                      properties=properties,
                                      descriptor_set=name),
                ).to_string()
            ))

            # This special column has a parameter, but is also listable,
            # but does not appear in the schema.
            columns.append(ColumnDefinition(
                column_name="_distance",
                type_name="double precision",
                options=ColumnOptions(
                    listable=True,
                    type="number",
                ).to_string()
            ))

        # Special field _label has a parameter, but is also listable,
        # and does appear in the schema, so we skip it here.

        columns.extend(blob_columns("_vector"))

        path_keys = get_path_keys(columns)

        options = TableOptions(
            table_name=f'descriptor."{table_name}"',
            count=properties["_count"],
            command="FindDescriptor",
            result_field="entities",
            modify_query=Curry(
                vector_selector, {"with_class": name, "distances": True}, indexes=properties["_indexes"]),
            path_keys=path_keys,
        )

        logger.debug(
            f"Creating table {table_name} with options {options.to_string()} and columns {columns}")

        table = TableDefinition(
            table_name=table_name,
            columns=columns,
            options=options.to_string()
        )
        results.append(table)

    return results


def get_descriptor_sets():
    """Read vector definitions and retain model/fingerprint customer metadata."""
    status, response, _ = execute_query(
        [{"GetIndexes": {"target": "descriptor", "property": "_blob"}}])
    if status != 0:
        raise RuntimeError(f"Cannot read descriptor vector definitions: {response}")
    classes = get_classes("entities")
    by_class = {}
    for index in response[0]["GetIndexes"].get("indexes", []):
        by_class.setdefault(index["class"], []).append(index)
    result = {}
    for name, indexes in by_class.items():
        index = preferred_vector_index(indexes)
        if index is not None:
            result[name] = {**index.get("metadata", {}),
                            "_dimensions": index["params"]["dimensions"],
                            "_indexes": [{"kind": item["kind"], "params": {"metric": item["params"]["metric"]}}
                                         for item in indexes],
                            "_count": classes.get("descriptor:" + name, {}).get("matched", 0)}
    return result


def property_columns_for_descriptors_in_set(name):
    return property_columns(get_classes("entities").get("descriptor:" + name, {}))


def find_similar_modify_command_body(
        value: str, command_body: dict) -> None:
    """
    Modify the command body for a find similar query.

    Args:
        value: JSON string generated from the FIND_SIMILAR SQL function
        command_body: The command body to modify

    Side Effects:
        Modifies the command body in place to include the find similar parameters.
    """
    find_similar = value

    logger.debug(f"find_similar: {find_similar}")

    if not isinstance(find_similar, dict):
        raise ValueError(
            f"Invalid find_similar format: {find_similar}. Expected an object.")

    include_list = {"k_neighbors", "knn_first", "engine", "metric"}
    extra = {k: v for k, v in find_similar.items(
    ) if k in include_list and v is not None}
    command_body.update(extra)


def find_similar_query_blobs(
        properties: dict, descriptor_set: str,
        value: str) -> List[bytes]:
    """
    Generates vector data for find similar operations.

    Args:
        properties: The properties of the descriptor class.
        descriptor_set: The name of the descriptor class.
        value: JSON string generated from the FIND_SIMILAR SQL function

    Returns:
        blobs: list of length one containing the vector data as bytes
    """
    find_similar = value

    logger.debug(f"find_similar: {find_similar}")

    if not isinstance(find_similar, dict):
        raise ValueError(
            f"Invalid find_similar format: {find_similar}. Expected an object.")

    if "vector" in find_similar and find_similar["vector"] is not None:
        raw_vector = find_similar["vector"]
        dimensions = properties["_dimensions"]
        # Validate that raw_vector is a list or tuple of the correct length and numeric type
        if not isinstance(raw_vector, (list, tuple)):
            raise ValueError(
                f"Invalid vector type: {type(raw_vector)}. Expected list or tuple.")
        if len(raw_vector) != dimensions:
            raise ValueError(
                f"Invalid vector length: {len(raw_vector)}. Expected {dimensions}.")
        if not all(isinstance(x, (int, float)) for x in raw_vector):
            raise ValueError(
                f"Invalid vector contents: {raw_vector}. All elements must be numeric (int or float).")
        # convert to bytes
        vector = struct.pack(f"<{dimensions}f", *raw_vector)
    else:
        start_time = datetime.now()
        model_key = dict(
            provider=properties.get("embeddings_provider"),
            model=properties.get("embeddings_model"),
            corpus=properties.get("embeddings_pretrained")
        )
        if "text" in find_similar and find_similar["text"] is not None:
            text = find_similar["text"]
            assert isinstance(text, str), "Text must be a string"
            vector = embed_texts(**model_key, texts=[text])[0]
            assert isinstance(vector, bytes), "Vector must be bytes"
        elif "image" in find_similar and find_similar["image"] is not None:
            # BYTEA encoded to base64 by FIND_SIMILAR function
            image = base64.b64decode(find_similar["image"])
            assert isinstance(
                image, bytes), f"Image must be bytes, got {type(image)}"
            vector = embed_images(**model_key, images=[image])[0]
            assert isinstance(vector, bytes), "Vector must be bytes"
        else:
            raise ValueError(
                "find_similar must have one of 'text', 'image', or 'vector' to embed.")
        elapsed_time = datetime.now() - start_time
        logger.debug(
            f"Embedding took {elapsed_time.total_seconds()} seconds for descriptor class {descriptor_set}")

    return [vector]  # Return as a list of one blob


def vector_selector(parameters, query, indexes=None):
    body = next(iter(query[0].values()))
    if indexes is not None and "k_neighbors" in body:
        index = preferred_vector_index(indexes, body.get("engine"), body.get("metric"))
        if index is None:
            raise ValueError("No descriptor index matches the requested engine and metric")
        body["engine"] = dict(ENGINE_ORDER)[index["kind"]]
        metrics = index["params"]["metric"]
        body.setdefault("metric", metrics[0] if isinstance(metrics, list) else metrics)
    for key, value in parameters.items():
        if key == "with_class":
            body[key] = value
        else:
            body.setdefault(key, value)
