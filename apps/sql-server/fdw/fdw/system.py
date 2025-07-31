from typing import List, Literal
from .common import property_columns, SCHEMA, TYPE_MAP, TableOptions, ColumnOptions
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
    if "entities" in SCHEMA and "classes" in SCHEMA["entities"]:
        assert isinstance(SCHEMA["entities"]["classes"], dict), \
            f"Expected entities.classes to be a dict, got {type(SCHEMA['entities']['classes'])}"
        for entity, data in SCHEMA["entities"]["classes"].items():
            if entity[0] == "_":
                results.append(system_table(entity, data))

    # We include special tables for class-independent entities and connections
    # These don't appear in the results of GetSchema.
    results.append(system_entity_table())
    results.append(system_connection_table())

    return results


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
            class_=entity,
            type=entity,
            matched=data.get("matched", 0),
            command=f"Find{entity[1:]}",  # e.g. FindBlob, FindImage, etc.
            result_field="entities",
        )

        # Blob-like entities get special columns specific to the type
        if entity == "_Blob":
            # _Blob gets _blob column
            columns.append(ColumnDefinition(
                column_name="_blob", type_name="bytea",
                options=ColumnOptions(
                    count=data.get("matched", 0), indexed=False, type="blob", listable=False).to_string())
            )
            options.blob_column = "_blob"
        elif entity == "_Image":
            # _Image gets _image, _as_format, _operations columns
            columns.append(ColumnDefinition(
                column_name="_image", type_name="bytea",
                options=ColumnOptions(
                    count=data.get("matched", 0), indexed=False, type="blob", listable=False).to_string())
            )
            options.blob_column = "_image"
            columns.append(ColumnDefinition(
                column_name="_as_format", type_name="image_format_enum",
                options=ColumnOptions(
                    count=data.get("matched", 0), indexed=False, type="string", listable=False).to_string())
            )
            columns.append(ColumnDefinition(
                column_name="_operations", type_name="jsonb",
                options=ColumnOptions(
                    count=data.get("matched", 0), indexed=False, type="json", listable=False).to_string())
            )
            options.blob_column = "_image"
            options.operation_types = ["threshold",
                                       "resize", "crop", "rotate", "flip"]

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

    options = TableOptions(
        type="entity",
        command=f"FindEntity",
        result_field="entities",
    )

    columns = [
        ColumnDefinition(
            column_name="_uniqueid", type_name="text",
            options=ColumnOptions(count=0, indexed=True, unique=True, type="string").to_string()),
    ]

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

    options = TableOptions(
        type="connection",
        command="FindConnection",
        result_field="connections",
    )

    columns = [
        ColumnDefinition(
            column_name="_uniqueid", type_name="text",
            options=ColumnOptions(count=0, indexed=True, unique=True, type="string").to_string()),
    ]

    columns.extend(get_consistent_properties("connections"))

    # Add the _src, and _dst columns
    columns.append(ColumnDefinition(
        column_name="_src", type_name="text", options=ColumnOptions(indexed=True, type="string").to_string()))
    columns.append(ColumnDefinition(
        column_name="_dst", type_name="text", options=ColumnOptions(indexed=True, type="string").to_string()))

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
    Get column definitions for properties that are consistent across all classes of a given type.
    """
    property_types = defaultdict(set)
    if type_ in SCHEMA and "classes" in SCHEMA[type_]:
        for data in SCHEMA[type_]["classes"].values():
            if "properties" in data and data["properties"] is not None:
                assert isinstance(data["properties"], dict), \
                    f"Expected properties to be a dict, got {type(data['properties'])}"
                for prop, prop_data in data["properties"].items():
                    count, indexed, type_ = prop_data
                    property_types[prop].add(type_.lower())

    columns = []
    for prop, types in property_types.items():
        if len(types) == 1:
            type_ = types.pop()
            if type_ in TYPE_MAP:
                options = ColumnOptions(
                    count=0, indexed=False, type=type_)
                columns.append(ColumnDefinition(
                    column_name=prop,
                    type_name=TYPE_MAP[type_],
                    options=options.to_string()))
            else:
                logger.warning(
                    f"Unknown type '{type_}' for property '{prop}' in system {type_} table.")
        else:
            logger.warning(
                f"Property '{prop}' has multiple types: {types}. Skipping.")
    return columns
