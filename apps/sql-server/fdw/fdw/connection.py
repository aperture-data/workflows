import logging
from typing import List
from .common import property_columns, get_schema, TableOptions, ColumnOptions
from multicorn import TableDefinition, ColumnDefinition

logger = logging.getLogger(__name__)


def connection_schema() -> List[TableDefinition]:
    """
    Return the connection schema for ApertureDB.
    This is used to create the foreign tables for connection classes.
    Here we create a separate table for each connection class.
    """
    logger.info("Creating connection schema")
    results = []
    schema = get_schema()
    if "connections" in schema and "classes" in schema["connections"]:
        assert isinstance(schema["connections"]["classes"], dict), \
            f"Expected connections.classes to be a dict, got {type(schema['connections']['classes'])}"
        for connection, data in schema["connections"]["classes"].items():
            results.append(connection_table(connection, data))
    return results


def connection_table(connection: str, data: dict) -> TableDefinition:
    """
    Create a TableDefinition for a connection.
    This is used to create the foreign table in PostgreSQL.
    """

    table_name = connection

    options = TableOptions(
        class_=connection,
        type="connection",
        count=data.get("matched", 0),
        command="FindConnection",
        result_field="connections",
    )

    columns = []

    try:
        columns.extend(property_columns(data))

        # Add the _src, and _dst columns
        columns.append(ColumnDefinition(
            column_name="_src", type_name="text", options=ColumnOptions(class_=data["src"], count=data.get("matched", 0), indexed=True, type="string").to_string()))
        columns.append(ColumnDefinition(
            column_name="_dst", type_name="text", options=ColumnOptions(class_=data["dst"], count=data.get("matched", 0), indexed=True, type="string").to_string()))
    except Exception as e:
        logger.exception(
            f"Error processing properties for connection {connection}: {e}")
        raise

    logger.debug(
        f"Creating connection table for {connection} with columns: {columns} and options: {options}")

    return TableDefinition(
        table_name=table_name,
        columns=columns,
        options=options.to_string()
    )
