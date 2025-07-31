from multicorn import TableDefinition, ColumnDefinition
from typing import List
from .common import property_columns, SCHEMA, POOL, TableOptions
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
    assert isinstance(descriptor_sets, dict), \
        f"Expected descriptor_sets to be a dict, got {type(descriptor_sets)}"
    for name, properties in descriptor_sets.items():
        table_name = name

        options = TableOptions(
            class_="_Descriptor",
            type="entity",
            count=properties["_count"],
            command="FindDescriptor",
            result_field="entities",
            extra={"with_name": name},
            descriptor_set_properties=properties,
        )

        table = TableDefinition(
            table_name=table_name,
            columns=columns,
            options=options.to_string()
        )
        results.append(table)
    return results
