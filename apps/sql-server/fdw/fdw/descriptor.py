from multicorn import TableDefinition, ColumnDefinition
from typing import List
from .common import property_columns, get_schema, get_pool, TableOptions, ColumnOptions, import_from_app
import logging

with import_from_app():
    from embeddings import Embedder


logger = logging.getLogger(__name__)


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

    properties = response[1]["GetSchema "].get(
        "entities", {}).get("classes", {}).get("_Descriptor", {})

    columns = property_columns(properties)

    return columns


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
            class_="_Descriptor",
            type="entity",
            count=properties["_count"],
            command="FindDescriptor",
            result_field="entities",
            extra={"set": name},
            descriptor_set_properties=properties,
            descriptor_set=name,
            find_similar=find_similar,
        )

        columns = property_columns_for_descriptors_in_set(name)

        if find_similar:
            columns.append(ColumnDefinition(
                column_name="_find_similar",
                type_name="JSONB",
                options=ColumnOptions(type="json", listable=False).to_string()
            ))

        logger.debug(
            f"Creating table {table_name} with options {options.to_string()} and columns {columns}")

        table = TableDefinition(
            table_name=table_name,
            columns=columns,
            options=options.to_string()
        )
        results.append(table)
    return results
