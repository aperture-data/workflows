import os
from typing import List, Annotated

from pydantic import BaseModel, Field

from shared import logger, connection_pool
from decorators import declare_mcp_tool
from aperturedb.Utils import Utils


def get_schema():
    """Get the schema of the ApertureDB database."""
    with connection_pool.get_utils() as utils:
        schema = utils.get_schema()
    return schema


@declare_mcp_tool
def list_entity_classes() -> List[str]:
    """List all entity classes in the database."""
    schema = get_schema()
    try:
        results = schema['entities']['classes'].keys()
    except KeyError:
        logger.error("Schema does not contain 'entities' or 'classes'.")
        raise ValueError("Schema does not contain 'entities' or 'classes'.")
    return list(results)


@declare_mcp_tool
def describe_entity_class(
    class_name: Annotated[str, Field(
        description="The name of the entity class to describe")]
) -> dict:
    """Describe an entity class in the database."""
    schema = get_schema()
    try:
        description = schema['entities']['classes'][class_name]
    except KeyError:
        logger.error(f"Entity class '{class_name}' not found in schema.")
        raise ValueError(f"Entity class '{class_name}' not found in schema.")
    return description


@declare_mcp_tool
def list_connection_classes() -> List[str]:
    """List all connection classes in the database."""
    schema = get_schema()
    try:
        results = schema['connections']['classes'].keys()
    except KeyError:
        logger.error("Schema does not contain 'connections' or 'classes'.")
        raise ValueError("Schema does not contain 'connections' or 'classes'.")
    return list(results)


@declare_mcp_tool
def describe_connection_class(
    class_name: Annotated[str, Field(
        description="The name of the connection class to describe")]
) -> dict:
    """Describe an connection class in the database."""
    schema = get_schema()
    try:
        description = schema['connections']['classes'][class_name]
    except KeyError:
        logger.error(f"Connection class '{class_name}' not found in schema.")
        raise ValueError(
            f"Connection class '{class_name}' not found in schema.")
    return description
