# This module populates the connection schema for ApertureDB.
# This schema contains a table for each connection class.
# Hence every AQL query includes `with_class`.
# In addition to the usual _uniqueid, tables have columns _src and _dst.
#
# SELECT _uniqueid, _src, _dst
# FROM "WorkflowCreated";

import logging
from typing import List
from .common import get_classes, Curry
from .column import property_columns, ColumnOptions, get_path_keys
from .table import TableOptions, literal
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
    classes = get_classes("connections")
    for connection, data in classes.items():
        results.append(connection_table(connection, data))
    return results


def connection_table(connection: str, data: dict) -> TableDefinition:
    """
    Create a TableDefinition for a connection.
    This is used to create the foreign table in PostgreSQL.
    """

    table_name = connection

    columns = []
    is_system_class = connection[0] == "_"

    try:
        if not is_system_class:
            columns.extend(property_columns(data))

        # Add the _src, and _dst columns
        columns.append(ColumnDefinition(
            column_name="_src",
            type_name="text",
            options=ColumnOptions(
                count=data.get("matched", 0),
                indexed=True,
                type="uniqueid",
            ).to_string()))
        columns.append(ColumnDefinition(
            column_name="_dst",
            type_name="text",
            options=ColumnOptions(
                count=data.get("matched", 0),
                indexed=True,
                type="uniqueid",
            ).to_string()))
    except Exception as e:
        logger.exception(
            f"Error processing properties for connection {connection}: {e}")
        raise

    path_keys = get_path_keys(columns)

    options = TableOptions(
        table_name=f'connection."{table_name}"',
        count=data.get("matched", 0),
        command="FindConnection",
        result_field="connections",
        modify_query=Curry(connection, class_name=connection,
                           src_class=data["src"], dst_class=data["dst"]),
        path_keys=path_keys,
    )

    logger.debug(
        f"Creating connection table for {connection} with columns: {columns} and options: {options}")

    return TableDefinition(
        table_name=table_name,
        columns=columns,
        options=options.to_string())
