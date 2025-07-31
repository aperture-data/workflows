import logging
from typing import List
from .common import property_columns, encode_options, SCHEMA
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
    if "connections" in SCHEMA and "classes" in SCHEMA["connections"]:
        for connection, data in SCHEMA["connections"]["classes"].items():
            results.append(connection_table(connection, data))
    return results


def connection_table(connection: str, data: dict) -> TableDefinition:
    """
    Create a TableDefinition for a connection.
    This is used to create the foreign table in PostgreSQL.
    """
    columns = []

    try:
        columns.extend(property_columns(data))

        # Add the _src, and _dst columns
        columns.append(ColumnDefinition(
            column_name="_src", type_name="text", options=encode_options({"class": data["src"], "count": data["matched"], "indexed": True, "type": "string"})))
        columns.append(ColumnDefinition(
            column_name="_dst", type_name="text", options=encode_options({"class": data["dst"], "count": data["matched"], "indexed": True, "type": "string"})))
    except Exception as e:
        logger.exception(
            f"Error processing properties for connection {connection}: {e}")
        raise

    options = {
        "class": connection,
        "type": "connection",
        "matched": data["matched"],
        "command": "FindConnection",
        "result_field": "connections",
    }

    logger.debug(
        f"Creating connection table for {connection} with columns: {columns} and options: {options}")

    return TableDefinition(
        table_name=connection,
        columns=columns,
        options=encode_options(options)
    )
