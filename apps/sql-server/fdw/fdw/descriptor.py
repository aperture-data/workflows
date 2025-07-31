from multicorn import TableDefinition, ColumnDefinition
from typing import List
from .common import property_columns, encode_options, SCHEMA
import logging

logger = logging.getLogger(__name__)


def get_descriptor_sets() -> dict:
    """
    Get the descriptor sets from the environment variable.
    This is used to create the foreign tables for descriptor sets.
    """
    query = [{"FindDescriptorSet": {"results": {"all_properties": True},
                                    "counts": True, "engines": True, "dimensions": True, "metrics": True}}]
    _, response, _ = POOL.execute_query(query)
    if "entities" not in response[0]["FindDescriptorSet"]:
        return {}

    results = {
        e['_name']: e for e in response[0]["FindDescriptorSet"]["entities"]
    }
    return results


def descriptor_schema() -> List[TableDefinition]:
    """
    Return the descriptor set schema for ApertureDB.
    This is used to create the foreign tables for descriptors.
    Here we create a separate table for each descriptor set.
    """
    logger.info("Creating descriptor schema")
    results = []
    descriptor_sets = get_descriptor_sets()
    columns = property_columns(SCHEMA.get("_DescriptorSet", {}))
    for name, properties in descriptor_sets.items():
        options = {
            "class": "_Descriptor",
            "type": "entity",
            "matched": properties["_count"],
            "command": "FindDescriptor",
            "result_field": "entities",
            "extra": {"with_name": name},
            "descriptor_set_properties": properties,
        }

        table = TableDefinition(
            table_name=name,
            columns=columns,
            options=encode_options(options)
        )
        results.append(table)
    return results
