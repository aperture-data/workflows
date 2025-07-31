from multicorn import TableDefinition
from .common import property_columns, encode_options, SCHEMA
from typing import List
import logging

logger = logging.getLogger(__name__)


def entity_table(entity: str, data: dict) -> TableDefinition:
    """
    Create a TableDefinition for an entity.
    This is used to create the foreign table in PostgreSQL.
    Here we create a separate table for each entity class.
    """

    assert entity[0] != "_", f"Entity name '{entity}' should not start with an underscore."

    columns = []

    try:
        columns.extend(property_columns(data))
    except Exception as e:
        logger.exception(
            f"Error processing properties for entity {entity}: {e}")
        raise

    table_name = entity

    options = {
        "class": entity,
        "type": "entity",
        "matched": data["matched"],
        "extra": {"with_class": entity},
        "command": "FindEntity",
        "result_field": "entities",
    }

    logger.debug(
        f"Creating entity table for {entity} as {table_name} with columns: {columns} and options: {options}")

    return TableDefinition(
        table_name=table_name,
        columns=columns,
        options=encode_options(options)
    )


def entity_schema() -> List[TableDefinition]:
    """
    Return the entity schema for ApertureDB.
    This is used to create the foreign tables for entity classes.
    """
    logger.info("Creating entity schema")
    results = []
    if "entities" in SCHEMA and "classes" in SCHEMA["entities"]:
        for entity, data in SCHEMA["entities"]["classes"].items():
            if entity[0] != "_":
                results.append(entity_table(entity, data))
    return results
