# This module populates the connection schema for ApertureDB.
# This schema contains a table for each connection class.
# Hence every AQL query includes `with_class`.
# In addition to the usual _uniqueid, tables have columns _src and _dst.
#
# SELECT _uniqueid, _src, _dst
# FROM "WorkflowCreated";

import logging
from typing import List, Union, Optional
from .common import Curry
from .column import property_columns, ColumnOptions, get_path_keys
from .table import TableOptions, literal, connection as table_connection
from .aperturedb import get_classes
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
        result = connection_table(connection, data)
        if result is not None:
            results.append(result)
    return results


def connection_table(connection: str, data: Union[dict, list]) -> Optional[TableDefinition]:
    """
    Create a TableDefinition for a connection.
    This is used to create the foreign table in PostgreSQL.

    Note that data is a list from 0.18.15 onwards, but commonly of length 1
    """

    table_name = connection

    columns = []
    is_system_class = connection[0] == "_"

    try:
        # We have to handle three cases here: dict, list of length 1, and list of length > 1
        if isinstance(data, dict):
            data = [data] # normalize to list form

        assert isinstance(data, list), "Expected data to be a list"

        if len(data) == 1:
            count = data[0].get("matched", 0)
            src_class = data[0].get("src", None)
            dst_class = data[0].get("dst", None)
            if not is_system_class:
                columns.extend(property_columns(data[0]))
        else: # TODO: two or more items; skip for now
            return None


        # Add the _src, and _dst columns
        columns.append(ColumnDefinition(
            column_name="_src",
            type_name="text",
            options=ColumnOptions(
                count=count,
                indexed=True,
                type="uniqueid",
            ).to_string()))
        columns.append(ColumnDefinition(
            column_name="_dst",
            type_name="text",
            options=ColumnOptions(
                count=count,
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
        count=count,
        command="FindConnection",
        result_field="connections",
        modify_query=Curry(table_connection, class_name=connection,
            src_class=src_class, dst_class=dst_class),
        path_keys=path_keys,
    )

    logger.debug(
        f"Creating connection table for {connection} with columns: {columns} and options: {options}")

    return TableDefinition(
        table_name=table_name,
        columns=columns,
        options=options.to_string())
