from .common import Curry, get_command_body, PathKey
from .aperturedb import get_classes
import logging
from pydantic import BaseModel
from typing import List, Dict, Any, Optional, Tuple, Callable, Literal, Iterable

logger = logging.getLogger(__name__)


class TableOptions(BaseModel):
    """
    Options passed to the foreign table from `import_schema`.
    """
    # name of the table in PostgreSQL
    table_name: str
    # number of objects, probably from "matched" field in GetSchema response
    count: int = 0
    # command to execute, e.g. "FindEntity", "FindConnection", etc.
    command: str = "FindEntity"
    # field to look for in the response, e.g. "entities", "connections"
    result_field: str = "entities"
    # path keys for the table; see https://github.com/pgsql-io/multicorn2/blob/7ab7f0bcfe6052ebb318ed982df8dfd78ce5ee6a/python/multicorn/__init__.py#L215
    path_keys: List[PathKey] = []

    # This hook is used to modify the command body before executing it.
    # It is passed the command body as `command_body`.
    # It should modify the command body in place.
    modify_query: Optional[Curry] = None

    def model_post_init(self, context: Any):
        """
        Validate the options after model initialization.
        """
        # Check that modify_query has a valid function signature
        if self.modify_query:
            self.modify_query.validate_signature({"query"})

    @classmethod
    def from_string(cls, options_str: Dict[str, str]) -> "TableOptions":
        """
        Create a TableOptions instance from a string dictionary.
        This is used to decode options from the foreign table definition.
        Postgres restricts options to be a string-valued dictionary.
        """
        return cls.model_validate_json(options_str["table_options"])

    def to_string(self) -> Dict[str, str]:
        """
        Convert TableOptions to a string dictionary.
        This is used to encode options for the foreign table definition.
        """
        return {"table_options": self.model_dump_json()}

    # Reject any extra fields that are not defined in the model.
    model_config = {
        "extra": "forbid"
    }


# Utility functions for Curry hooks


def literal(parameters: Dict[str, Any],
            query: List[dict]) -> Optional[Callable]:
    """
    A TableOptions modify_query hook.

    Adds the value to the query under the given name.
    This is used to modify the query before executing it.
    """
    command_body = get_command_body(query[0])
    command_body.update(parameters)
    return None


def connection(class_name, src_class, dst_class, query):
    """Push known typed endpoint filters through ordinary FindConnection.

    Global/mixed-category endpoint qualifications remain PostgreSQL filters;
    selecting all candidate connections preserves SQL semantics without treating
    ownership as a customer connection or guessing endpoint types.
    """
    body = get_command_body(query[0])
    condition = body.get("constraints")
    terms = condition[1:] if condition and condition[0] == "all" else [
        condition] if condition else []
    endpoint_terms = {"_src": [], "_dst": []}
    ordinary = []
    for term in terms:
        name = term[0][1:] if isinstance(term, list) and isinstance(term[0], str) else None
        if name in endpoint_terms:
            endpoint_terms[name].append(term)
        else:
            ordinary.append(term)
    if ordinary:
        body["constraints"] = ordinary[0] if len(ordinary) == 1 else ["all", *ordinary]
    else:
        body.pop("constraints", None)
    if class_name:
        body["with_class"] = class_name
    before = []
    commands = {"entity": "Entity", "image": "Image", "video": "Video", "blob": "Blob",
                "descriptor": "Descriptor", "frame": "Frame", "clip": "Clip",
                "bounding_box": "BoundingBox", "polygon": "Polygon"}
    for endpoint, selector in (("_src", src_class), ("_dst", dst_class)):
        if not selector or not endpoint_terms[endpoint]:
            continue
        target, category = selector.split(":", 1)
        reference = len(before) + 1
        filters = [["$_uniqueid", *term[1:]] for term in endpoint_terms[endpoint]]
        before.append({"Find" + commands[target]: {"with_class": category, "_ref": reference,
                                                   "constraints": filters[0] if len(filters) == 1 else ["all", *filters]}})
        body[endpoint[1:]] = reference
    query[:] = before + [query[0]]
    return None
