import os
from typing import List, Annotated, Optional, Dict

from pydantic import BaseModel, Field

from shared import logger, connection_pool
from decorators import declare_mcp_tool
from aperturedb.Utils import Utils


class ClassList(BaseModel):
    classes: List[str]


class PropertyDescription(BaseModel):
    matched: int
    indexed: bool
    type: str


class EntityClassDescription(BaseModel):
    matched: int
    properties: Dict[str, PropertyDescription]


class ConnectionClassDescription(BaseModel):
    matched: int
    properties: Dict[str, PropertyDescription]
    src: str
    dst: str


class ConnectionClassDescriptions(BaseModel):
    items: List[ConnectionClassDescription]


def get_schema():
    """Get the schema of the ApertureDB database."""
    with connection_pool.get_connection() as client:
        utils = Utils(client)
        schema = utils.get_schema()
    return schema



@declare_mcp_tool
def list_entity_classes() -> ClassList:
    """List all entity classes in the database."""
    schema = get_schema()
    try:
        results = schema['entities']['classes'].keys()
    except KeyError:
        logger.error("Schema does not contain 'entities' or 'classes'.")
        raise ValueError("Schema does not contain 'entities' or 'classes'.")
    return ClassList(classes=list(results))


@declare_mcp_tool
def describe_entity_class(
    class_name: Annotated[str, Field(
        description="The name of the entity class to describe")]
) -> EntityClassDescription:
    """Describe an entity class in the database."""
    schema = get_schema()
    try:
        description = schema['entities']['classes'][class_name]
    except KeyError:
        logger.error(f"Entity class '{class_name}' not found in schema.")
        raise ValueError(f"Entity class '{class_name}' not found in schema.")
    return EntityClassDescription(matched=description['matched'], properties={k: PropertyDescription(matched=v[0], indexed=v[1], type=v[2]) for k, v in (description.get('properties') or {}).items()})


@declare_mcp_tool
def list_connection_classes() -> ClassList:
    """List all connection classes in the database."""
    schema = get_schema()
    try:
        results = schema['connections']['classes'].keys()
    except KeyError:
        logger.error("Schema does not contain 'connections' or 'classes'.")
        raise ValueError("Schema does not contain 'connections' or 'classes'.")
    return ClassList(classes=list(results))


@declare_mcp_tool
def describe_connection_class(
    class_name: Annotated[str, Field(
        description="The name of the connection class to describe")]
) -> ConnectionClassDescriptions:
    """Describe an connection class in the database."""
    schema = get_schema()
    try:
        # From athena 0.18.15, connection class values are lists of dicts
        # so we standardize on that format
        description = schema['connections']['classes'][class_name]
        if isinstance(description, dict):
            description = [description]
    except KeyError:
        logger.error(f"Connection class '{class_name}' not found in schema.")
        raise ValueError(
            f"Connection class '{class_name}' not found in schema.")
    return ConnectionClassDescriptions(items=[ConnectionClassDescription(matched=d['matched'], properties={k: PropertyDescription(matched=v[0], indexed=v[1], type=v[2]) for k, v in (d.get('properties') or {}).items()}, src=d['src'], dst=d['dst']) for d in description])
