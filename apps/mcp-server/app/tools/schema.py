from typing import Annotated, Dict, List

from pydantic import BaseModel, Field

from shared import connection_pool
from decorators import declare_mcp_tool


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


@declare_mcp_tool
def list_entity_classes() -> ClassList:
    """List all entity classes in the database."""
    with connection_pool.get_utils() as utils:
        schema = utils.get_schema()
    return ClassList(classes=list((schema.get('entities') or {}).get('classes', {})))


@declare_mcp_tool
def describe_entity_class(
    class_name: Annotated[str, Field(
        description="The name of the entity class to describe")]
) -> EntityClassDescription:
    """Describe an entity class, its properties, and their indexes."""
    with connection_pool.get_utils() as utils:
        schema = utils.get_schema()
        try:
            description = schema['entities']['classes'][class_name]
        except (KeyError, TypeError):
            raise ValueError(f"Entity class '{class_name}' not found in schema.") from None
        indexed = {index['property'] for index in
                   utils.get_indexes(target='entity', class_name=class_name)}
    return EntityClassDescription(
        matched=description['matched'],
        properties={
            name: PropertyDescription(matched=info['count'],
                                      indexed=name in indexed, type=info['type'])
            for name, info in (description.get('properties') or {}).items()
        })


@declare_mcp_tool
def list_connection_classes() -> ClassList:
    """List all connection classes in the database."""
    with connection_pool.get_utils() as utils:
        schema = utils.get_schema()
    return ClassList(classes=list((schema.get('connections') or {}).get('classes', {})))


@declare_mcp_tool
def describe_connection_class(
    class_name: Annotated[str, Field(
        description="The name of the connection class to describe")]
) -> ConnectionClassDescriptions:
    """Describe a connection class, its properties, and their indexes."""
    with connection_pool.get_utils() as utils:
        schema = utils.get_schema()
        try:
            descriptions = schema['connections']['classes'][class_name]
        except (KeyError, TypeError):
            raise ValueError(f"Connection class '{class_name}' not found in schema.") from None
        indexed = {index['property'] for index in
                   utils.get_indexes(target='connection', class_name=class_name)}
    return ConnectionClassDescriptions(items=[
        ConnectionClassDescription(
            matched=description['matched'], src=description['src'], dst=description['dst'],
            properties={
                name: PropertyDescription(matched=info['count'],
                                          indexed=name in indexed, type=info['type'])
                for name, info in (description.get('properties') or {}).items()
            })
        for description in descriptions
    ])
