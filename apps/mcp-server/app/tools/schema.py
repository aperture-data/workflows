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
    with connection_pool.get_utils() as utils:
        schema = utils.get_schema()
    return ClassList(
        classes=[
            entry["class"] for entry in schema.get(
                "nodes",
                []) if entry["type"] == "entity"])


@declare_mcp_tool
def describe_entity_class(class_name: Annotated[str, Field(
        description="Entity class name")]) -> EntityClassDescription:
    with connection_pool.get_utils() as utils:
        description = next(
            (entry for entry in utils.get_schema().get(
                "nodes",
                []) if entry["type"] == "entity" and entry["class"].casefold() == class_name.casefold()),
            None)
        if description is None:
            raise ValueError(f"Entity class '{class_name}' not found in schema.")
        indexed = {index["property"]
                   for index in utils.get_indexes(target="entity", class_name=class_name)}
    return EntityClassDescription(matched=description["matched"], properties={
        name: PropertyDescription(matched=info["count"],
                                  indexed=name in indexed, type=info["type"])
        for name, info in description.get("properties", {}).items()})


@declare_mcp_tool
def list_connection_classes() -> ClassList:
    with connection_pool.get_utils() as utils:
        schema = utils.get_schema()
    return ClassList(classes=sorted({entry["class"]["connection"] for entry in schema.get("edges", [])}))


@declare_mcp_tool
def describe_connection_class(class_name: Annotated[str, Field(
        description="Connection class name")]) -> ConnectionClassDescriptions:
    with connection_pool.get_utils() as utils:
        descriptions = [entry for entry in utils.get_schema().get("edges", [])
                        if entry["class"]["connection"].casefold() == class_name.casefold()]
        if not descriptions:
            raise ValueError(f"Connection class '{class_name}' not found in schema.")
        items = []
        for description in descriptions:
            target = description["target"]
            classes = description["class"]
            indexed = {index["property"] for index in utils.get_indexes(target=target, class_name=classes)}
            items.append(
                ConnectionClassDescription(
                    matched=description["matched"],
                    src=target["src"] + ":" + classes["src"],
                    dst=target["dst"] + ":" + classes["dst"],
                    properties={
                        name: PropertyDescription(
                            matched=info["count"],
                            indexed=name in indexed,
                            type=info["type"]) for name,
                        info in description.get(
                            "properties",
                            {}).items()}))
    return ConnectionClassDescriptions(items=items)
