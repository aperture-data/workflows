# This module populates the system schema for ApertureDB.
# This schema contains tables for system classes such as _Blob, _Image, etc.
# Note that the tables for these are named without the leading underscore.
# It also includes special tables for Entity and Connection.
#
# SELECT * FROM "DescriptorSet" LIMIT 10;

from typing import List, Literal, Set, Any, Dict
from .common import get_classes, TYPE_MAP, Curry
from .column import property_columns, ColumnOptions, blob_columns, uniqueid_column, passthrough
from .table import TableOptions, literal
from multicorn import TableDefinition, ColumnDefinition
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


def system_schema() -> List[TableDefinition]:
    """
    Return the system schema for ApertureDB.
    This is used to create the foreign tables for system classes.
    Here we create a separate table for each system class, such as _Blob, _Image, etc.
    and also special tables for Entity and Connection.
    Note that we drop the leading underscore from the table names.
    """
    logger.info("Creating system schema")
    results = []
    classes = get_classes("entities")
    for entity, data in classes.items():
        if entity[0] == "_":
            results.append(system_table(entity, data))

    # We include special tables for class-independent entities and connections
    # These don't appear in the results of GetSchema.
    results.append(system_entity_table())
    results.append(system_connection_table())

    return results


def operations_column(types: Set[str]) -> ColumnDefinition:
    return ColumnDefinition(
        column_name="_operations",
        type_name="jsonb",
        options=ColumnOptions(
            type="json",
            listable=False,
            modify_command_body=Curry(operations_passthrough, types=types),
        ).to_string()
    )


def operations_passthrough(types: Set[str],
                           value: Any, command_body: Dict[str, Any]) -> None:
    """
    This is a ColumnOptions modify_command_body hook.

    Pass through the operations from the query to the command body.
    Includes JSON conversion and some validation.

    Error messages are SQL-oriented.
    """
    operations = value
    if not isinstance(operations, list):
        raise ValueError(
            f"Operations must be an array, got {operations} {type(operations)}")

    for op in operations:
        if not isinstance(op, dict):
            raise ValueError(
                f"Invalid operation format: {op}. Expected an object.")
        if "type" not in op:
            raise ValueError(
                f"Operation must have 'type' field: {op}")
        if op["type"] not in types:
            raise ValueError(
                f"Invalid operation type: {op['type']}. Expected one of {types}")

    command_body["operations"] = operations


def blob_extra_columns() -> List[ColumnDefinition]:
    """
    Returns the columns for a _Blob entity.
    """
    return blob_columns("_blob")


def image_extra_columns() -> List[ColumnDefinition]:
    """
    Returns the columns for a _Image entity.
    """
    return blob_columns("_image") + [
        ColumnDefinition(
            column_name="_as_format",
            type_name="text",
            options=ColumnOptions(
                listable=False,
                modify_command_body=Curry(passthrough, "as_format"),
            ).to_string()
        ),
        operations_column({"threshold", "resize", "crop", "rotate", "flip"}),
    ]


OBJECT_COLUMNS_HANDLERS = {
    "_Blob": blob_extra_columns,
    "_Image": image_extra_columns,
}


def system_table(entity: str, data: dict) -> TableDefinition:
    """
    Create a TableDefinition for a system object.
    This is used to create the foreign table in PostgreSQL.
    """

    assert entity[0] == "_", f"System entity name '{entity}' should start with an underscore."

    columns = []
    blob_column = None

    try:
        columns.extend(property_columns(data))

        table_name = entity[1:]

        options = TableOptions(
            table_name=f'system."{table_name}"',
            count=data.get("matched", 0),
            command=f"Find{entity[1:]}",  # e.g. FindBlob, FindImage, etc.
            result_field="entities",
        )

        if entity in OBJECT_COLUMNS_HANDLERS:
            columns.extend(OBJECT_COLUMNS_HANDLERS[entity]())

    except Exception as e:
        logger.exception(
            f"Error processing properties for entity {entity}: {e}")
        raise

    logger.debug(
        f"Creating entity table for {entity} as {table_name} with columns: {columns} and options: {options}")

    return TableDefinition(
        table_name=table_name,
        columns=columns,
        options=options.to_string()
    )


def system_entity_table() -> TableDefinition:
    """
    Create a TableDefinition for the system entity table.
    This is used to create the foreign table in PostgreSQL for system entities.
    This table has all properties that are found in any entity class, provided that the types are compatible.

    Note that it not possible to determine the class of the entity from this table.
    """
    table_name = "Entity"

    classes = get_classes("entities")
    count = sum(
        data.get("matched", 0) for class_, data in classes.items() if class_[0] != "_"
    )

    options = TableOptions(
        table_name=f'system."{table_name}"',
        count=count,
        command=f"FindEntity",
        result_field="entities",
    )

    columns = []

    columns.append(uniqueid_column(count))

    columns.extend(get_consistent_properties("entities"))

    logger.debug(
        f"Creating system entity table as {table_name} with columns: {columns} and options: {options}")

    result = TableDefinition(
        table_name=table_name,
        columns=columns,
        options=options.to_string())

    logger.debug(f"System entity table created: {result}")
    return result


def system_connection_table() -> TableDefinition:
    """
    Create a TableDefinition for the system connection table.
    This is used to create the foreign table in PostgreSQL for system connections.
    This table has all properties that are found in any connection class, provided that the types are compatible.
    """
    table_name = "Connection"
    classes = get_classes("connections")
    count = sum(
        data.get("matched", 0) for class_, data in classes.items()
    )

    options = TableOptions(
        table_name=f'system."{table_name}"',
        count=count,
        command="FindConnection",
        result_field="connections",
    )

    columns = []

    columns.append(uniqueid_column(count))

    columns.extend(get_consistent_properties("connections"))

    # Add the _src, and _dst columns
    columns.append(ColumnDefinition(
        column_name="_src",
        type_name="text",
        options=ColumnOptions(
            indexed=True,
            type="string",
        ).to_string()))

    columns.append(ColumnDefinition(
        column_name="_dst",
        type_name="text",
        options=ColumnOptions(
            indexed=True,
            type="string",
        ).to_string()))

    logger.debug(
        f"Creating system connection table as {table_name} with columns: {columns} and options: {options}")

    result = TableDefinition(
        table_name=table_name,
        columns=columns,
        options=options.to_string()
    )

    logger.debug(f"System connection table created: {result}")
    return result


def get_consistent_properties(type_: Literal["entities", "connections"]) -> List[ColumnDefinition]:
    """
    Get column definitions for properties that are consistently typed across all classes of a given type.
    """
    property_types = defaultdict(set)
    classes = get_classes(type_)
    for data in classes.values():
        if "properties" in data and data["properties"] is not None:
            assert isinstance(data["properties"], dict), \
                f"Expected properties to be a dict, got {type(data['properties'])}"
            for prop, prop_data in data["properties"].items():
                count, indexed, prop_type = prop_data
                property_types[prop].add(prop_type.lower())

    columns = []
    for prop, types in property_types.items():
        if len(types) == 1:
            prop_type = types.pop()
            if prop_type in TYPE_MAP:
                options = ColumnOptions(
                    count=0, indexed=False, type=prop_type)
                columns.append(ColumnDefinition(
                    column_name=prop,
                    type_name=TYPE_MAP[prop_type],
                    options=options.to_string()))
            else:
                logger.warning(
                    f"Unknown type '{prop_type}' for property '{prop}' in system {type_} table.")
        else:
            logger.warning(
                f"Property '{prop}' has multiple types: {types}. Skipping.")
    return columns
