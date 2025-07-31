from typing import List, Literal
from .common import property_columns, encode_options, SCHEMA, TYPE_MAP
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

        # Blob-like entities get special columns specific to the type
        if entity == "_Blob":
            # _Blob gets _blob column
            columns.append(ColumnDefinition(
                column_name="_blob", type_name="bytea",
                options=encode_options({"count": data["matched"], "indexed": False, "type": "blob", "special": True})))
            blob_column = "_blob"
        elif entity == "_Image":
            # _Image gets _image, _as_format, _operations columns
            columns.append(ColumnDefinition(
                column_name="_image", type_name="bytea",
                options=encode_options({"count": data["matched"], "indexed": False, "type": "blob", "special": True})))
            columns.append(ColumnDefinition(
                column_name="_as_format", type_name="image_format_enum",
                options=encode_options({"count": data["matched"], "indexed": False, "type": "string", "special": True})))
            columns.append(ColumnDefinition(
                column_name="_operations", type_name="jsonb",
                options=encode_options({"count": data["matched"], "indexed": False, "type": "json", "special": True})))
            blob_column = "_image"
        # elif entity == "_BoundingBox":
        #     # _BoundingBox gets _area, _image, _as_format, _image_ref, _coordinates, _in_rectangle
        #     # Area has a parameter "areas", but it isn't required as it can be simply added to the result list
        #     # This is why _area is not special.
        #     columns.append(ColumnDefinition(
        #         column_name="_area", type_name="double precision",
        #         options=encode_options({"count": data["matched"], "indexed": False, "type": "number", "special": False})))
        #     # _image is the blob pixel data for the bounding box; only valid when _image_ref is constrained
        #     columns.append(ColumnDefinition(
        #         column_name="_image", type_name="bytea",
        #         options=encode_options({"count": data["matched"], "indexed": False, "type": "blob", "special": True})))
        #     # _as_format is the format of the image, e.g. "png", "jpeg", etc.; only valid when _image_ref is constrained, and _image is requested
        #     columns.append(ColumnDefinition(
        #         column_name="_as_format", type_name="image_format_enum",
        #         options=encode_options({"count": data["matched"], "indexed": False, "type": "string", "special": True})))
        #     # _image_ref is the _uniqueid of the corresponding image, coming from an explicit JOIN on _Image
        #     columns.append(ColumnDefinition(
        #         column_name="_image_ref", type_name="text",
        #         options=encode_options({"count": data["matched"], "indexed": False, "type": "string", "special": True})))
        #     # _coordinates is a JSONB object containing the coordinates of the bounding box; it has to be requested explicitly
        #     columns.append(ColumnDefinition(
        #         column_name="_coordinates", type_name="jsonb",
        #         options=encode_options({"count": data["matched"], "indexed": False, "type": "json", "special": True})))
        #     # JSON object with x, y, width, and height used for filtering
        #     columns.append(ColumnDefinition(
        #         column_name="_in_rectangle", type_name="jsonb",
        #         options=encode_options({"count": data["matched"], "indexed": False, "type": "json", "special": True})))
    except Exception as e:
        logger.exception(
            f"Error processing properties for entity {entity}: {e}")
        raise

    table_name = entity[1:]

    options = {
        "class": entity,
        "type": "entity",
        "matched": data["matched"],
        "command": f"Find{entity[1:]}",  # e.g. FindBlob, FindImage, etc.
        "result_field": "entities",
        "blob_column": blob_column,
    }

    logger.debug(
        f"Creating entity table for {entity} as {table_name} with columns: {columns} and options: {options}")

    return TableDefinition(
        table_name=table_name,
        columns=columns,
        options=encode_options(options)
    )


def system_entity_table() -> TableDefinition:
    """
    Create a TableDefinition for the system entity table.
    This is used to create the foreign table in PostgreSQL for system entities.
    This table has all properties that are found in any entity class, provided that the types are compatible.

    Note that it not possible to determine the class of the entity from this table.
    """
    columns = [
        ColumnDefinition(
            column_name="_uniqueid", type_name="text",
            options=encode_options({"count": 0, "indexed": True, "unique": True, "type": "string"})),
    ]

    columns.extend(get_consistent_properties("entities"))

    table_name = "Entity"

    options = {
        "class": "_Entity",
        "type": "entity",
        "matched": 0,
        "command": "FindEntity",
        "result_field": "entities",
    }

    logger.debug(
        f"Creating system entity table as {table_name} with columns: {columns} and options: {options}")

    return TableDefinition(
        table_name=table_name,
        columns=columns,
        options=encode_options(options)
    )


def system_connection_table() -> TableDefinition:
    """
    Create a TableDefinition for the system connection table.
    This is used to create the foreign table in PostgreSQL for system connections.
    This table has all properties that are found in any connection class, provided that the types are compatible.
    """
    columns = [
        ColumnDefinition(
            column_name="_uniqueid", type_name="text",
            options=encode_options({"count": 0, "indexed": True, "unique": True, "type": "string"})),
    ]

    columns.extend(get_consistent_properties("connections"))

    # Add the _src, and _dst columns
    columns.append(ColumnDefinition(
        column_name="_src", type_name="text", options=encode_options({"indexed": True, "type": "string"})))
    columns.append(ColumnDefinition(
        column_name="_dst", type_name="text", options=encode_options({"indexed": True, "type": "string"})))

    table_name = "Connection"

    options = {
        "class": "_Connection",
        "type": "connection",
        "matched": 0,
        "command": "FindConnection",
        "result_field": "connections",
    }

    logger.debug(
        f"Creating system connection table as {table_name} with columns: {columns} and options: {options}")

    return TableDefinition(
        table_name=table_name,
        columns=columns,
        options=encode_options(options)
    )


def get_consistent_properties(type_: Literal["entities", "connections"]) -> List[ColumnDefinition]:
    """
    Get column definitions for properties that are consistent across all classes of a given type.
    """
    property_types = defaultdict(set)
    if type_ in SCHEMA and "classes" in SCHEMA[type_]:
        for data in SCHEMA[type_]["classes"].values():
            if "properties" in data and data["properties"] is not None:
                for prop, prop_data in data["properties"].items():
                    count, indexed, type_ = prop_data
                    property_types[prop].add(type_.lower())

    columns = []
    for prop, types in property_types.items():
        if len(types) == 1:
            type_ = types.pop()
            if type_ in TYPE_MAP:
                columns.append(ColumnDefinition(
                    column_name=prop,
                    type_name=TYPE_MAP[type_],
                    options=encode_options({"count": 0, "indexed": False, "type": type_})))
            else:
                logger.warning(
                    f"Unknown type '{type_}' for property '{prop}' in system {type_} table.")
        else:
            logger.warning(
                f"Property '{prop}' has multiple types: {types}. Skipping.")
    return columns
