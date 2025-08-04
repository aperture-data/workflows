# This module populates the descriptor schema for ApertureDB.
# This schema contains a table for each descriptor set,
# and hence every AQL query includes `set`.
# These tables support find-similar queries.
#
# SELECT * FROM descriptor."crawl-to-rag"
# WHERE _find_similar = FIND_SIMILAR(text := 'find entity', k := 10)
# AND _blobs

from multicorn import TableDefinition, ColumnDefinition
from typing import List
from .common import get_schema, get_pool, import_from_app, Curry
from .column import property_columns, ColumnOptions, blob_columns
from .table import TableOptions, literal
import logging
import numpy as np
import json
from datetime import datetime

with import_from_app():
    from embeddings import Embedder


logger = logging.getLogger(__name__)


def descriptor_schema() -> List[TableDefinition]:
    """
    Return the descriptor set schema for ApertureDB.
    This is used to create the foreign tables for descriptors.
    Here we create a separate table for each descriptor set.
    """
    logger.info("Creating descriptor schema")
    results = []
    descriptor_sets = get_descriptor_sets()
    assert isinstance(descriptor_sets, dict), \
        f"Expected descriptor_sets to be a dict, got {type(descriptor_sets)}"
    for name, properties in descriptor_sets.items():
        table_name = name

        # We switch the "find similar" feature on if the descriptor set
        # has properties that allow us to find the correct embedding model.
        # Notionally we could allow direct vector queries regardless, but
        # this is a good heuristic to avoid unnecessary complexity.
        find_similar = Embedder.check_properties(properties)

        options = TableOptions(
            table_name=f'descriptor."{table_name}"',
            count=properties["_count"],
            command="FindDescriptor",
            result_field="entities",
            modify_command_body=Curry(
                literal, {"set": name, "distances": True}),
        )

        columns = property_columns_for_descriptors_in_set(name)

        if find_similar:
            columns.append(ColumnDefinition(
                column_name="_find_similar",
                type_name="JSONB",
                options=ColumnOptions(
                    listable=False,
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
                    extra={"distances": True}).to_string()
            ))

        # Special field _label has a parameter, but is also listable,
        # and does appear in the schema, so we skip it here.

        columns.extend(blob_columns("_vector"))

        logger.debug(
            f"Creating table {table_name} with options {options.to_string()} and columns {columns}")

        table = TableDefinition(
            table_name=table_name,
            columns=columns,
            options=options.to_string()
        )
        results.append(table)

    return results


def get_descriptor_sets() -> dict:
    """
    Get the descriptor sets from the environment variable.
    This is used to create the foreign tables for descriptor sets.
    """
    query = [{"FindDescriptorSet": {"results": {"all_properties": True},
                                    "counts": True, "engines": True, "dimensions": True, "metrics": True}}]
    _, response, _ = get_pool().execute_query(query)
    if "entities" not in response[0]["FindDescriptorSet"]:
        return {}

    results = {
        e['_name']: e for e in response[0]["FindDescriptorSet"]["entities"]
    }
    return results


def property_columns_for_descriptors_in_set(name: str) -> dict:
    """
    Get the property columns for a specific descriptor set.
    """
    query = [{
        "FindDescriptor": {
            "set": name,
            "_ref": 1
        }
    }, {
        "GetSchema": {
            "ref": 1
        }
    }]

    _, response, _ = get_pool().execute_query(query)

    properties = response[1]["GetSchema"].get(
        "entities", {}).get("classes", {}).get("_Descriptor", {})

    columns = property_columns(properties)

    return columns


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
    try:
        find_similar = json.loads(value)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Invalid JSON for _find_similar: {value}") from e

    logger.debug(f"find_similar: {find_similar}")

    if not isinstance(find_similar, dict):
        raise ValueError(
            f"Invalid find_similar format: {find_similar}. Expected an object.")

    include_list = {"k_neighbors", "knn_first"}
    extra = {k: v for k, v in find_similar.items(
    ) if k in include_list and v is not None}
    command_body.update(extra)


def find_similar_query_blobs(
        properties: dict, descriptor_set: str,
        value: str) -> bytes:
    """
    Generates vector data for find similar operations.

    Args:
        properties: The properties of the descriptor set.
        descriptor_set: The name of the descriptor set.
        value: JSON string generated from the FIND_SIMILAR SQL function

    Returns:
        blobs: list of length one containing the vector data as bytes       
    """
    try:
        find_similar = json.loads(value)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Invalid JSON for _find_similar: {value}") from e

    logger.debug(f"find_similar: {find_similar}")

    if not isinstance(find_similar, dict):
        raise ValueError(
            f"Invalid find_similar format: {find_similar}. Expected an object.")

    if "vector" in find_similar and find_similar["vector"] is not None:
        vector = np.array(find_similar["vector"])
        dimensions = properties["_dimensions"]
        if vector.shape != (dimensions,):
            raise ValueError(
                f"Invalid vector size: {vector.shape}. Expected {dimensions}.")
    else:
        # This takes ~7s the first time, but ~1s on subsequent calls because of a file cache.
        # Could consider caching the embedder and maybe even doing cache warmup,
        # but the peculiar invocation environment of Python within PostgreSQL
        # makes this tricky, because we can't consistently persist state across calls.
        start_time = datetime.now()
        embedder = Embedder.from_properties(
            properties=properties,
            descriptor_set=descriptor_set,
        )
        elapsed_time = datetime.now() - start_time
        logger.debug(
            f"Creating embedder took {elapsed_time.total_seconds()} seconds for descriptor set {descriptor_set}")

        start_time = datetime.now()
        if "text" in find_similar and find_similar["text"] is not None:
            text = find_similar["text"]
            vector = embedder.embed_text(text)
        elif "image" in find_similar and find_similar["image"] is not None:
            image = find_similar["image"]
            vector = embedder.embed_image(image)
        else:
            raise ValueError(
                "find_similar must have one of 'text', 'image', or 'vector' to embed.")
        elapsed_time = datetime.now() - start_time
        logger.debug(
            f"Embedding took {elapsed_time.total_seconds()} seconds for descriptor set {descriptor_set}")

    blob = vector.tobytes()
    return [blob]  # Return as a list of one blob
