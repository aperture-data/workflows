# This module populates the entity schema for ApertureDB.
# This schema contains a table for each entity class.
# Hence every AQL query includes `with_class`.
#
# SELECT * FROM "CrawlDocument" LIMIT 10;

from multicorn import TableDefinition
from .common import Curry
from .column import property_columns, get_path_keys
from .aperturedb import get_classes
from .table import TableOptions, literal
from typing import List
import logging

logger = logging.getLogger(__name__)


def entity_schema() -> List[TableDefinition]:
    """
    Return the entity schema for ApertureDB.
    This is used to create the foreign tables for entity classes.
    """
    logger.info("Creating entity schema")
    results = []
    classes = get_classes("entities")
    for entity, data in classes.items():
        if entity[0] != "_":
            results.append(entity_table(entity, data))
    return results


def entity_table(entity: str, data: dict) -> TableDefinition:
    """
    Create a TableDefinition for an entity.
    This is used to create the foreign table in PostgreSQL.
    Here we create a separate table for each entity class.
    """

    assert entity[0] != "_", f"Entity name '{entity}' should not start with an underscore."

    table_name = entity

    columns = []

    try:
        columns.extend(property_columns(data))
    except Exception as e:
        logger.exception(
            f"Error processing properties for entity {entity}: {e}")
        raise

    path_keys = get_path_keys(columns)

    options = TableOptions(
        table_name=f'entity."{table_name}"',
        count=data.get("matched", 0),
        command="FindEntity",
        result_field="entities",
        modify_query=Curry(literal, {"with_class": entity}),
        path_keys=path_keys,
    )

    logger.debug(
        f"Creating entity table for {entity} as {table_name} with columns: {columns} and options: {options}")

    return TableDefinition(
        table_name=table_name,
        columns=columns,
        options=options.to_string()
    )
