from .common import Curry, get_classes, get_command_body, PathKey
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


def connection(class_name: Optional[str],
               src_class: Optional[str], dst_class: Optional[str],
               query: List[dict]) -> Optional[Callable]:
    """
    A TableOptions modify_query hook.

    Handles the rewriting of a FindConnection query.
    This deals with the following restrictions:
    - FindConnection/with_class cannot be used with a system-defined class.
    - FindConnection does not support constraints on _src and _dst.

    Constraints on _src and _dst are moved to constraints on _uniqueid in a preceding Find<Object> command.
    Queries that are on system classes, or which neither request nor constrain other connection properties
    are converted to a pair of Find<Object> commands using "is_connected_to".

    So the input query is a single FindConnection command, and the output has one of these six forms:
    1. Single FindConnection command
    2. A Find<Object> command ref-tied to `src` in FindConnection
    3. A Find<Object> command ref-tied to `dst` in FindConnection
    4. Two Find<Object> commands ref-tied respectively to `src` and `dst` in FindConnection
    5. Two Find<Object> commands ref-tied via `is_connected_to` with direction `out`
    6. Two Find<Object> commands ref-tied via `is_connected_to` with direction `in`

    For 5 and 6, `group_by_source` is also used, and it is necessary to rewrite the results accordingly
    because the results will be a dictionary rather than a flat list. This hook optionally returns a function
    that takes the response and returns the rewritten results.
    """
    # The three questions we ask are:
    # Is this a system class?
    # Does it request or constrain _src or _dst?
    # Does it request or constrain any other connection properties?
    is_system_class = class_name and class_name.startswith("_")
    assert "FindConnection" in query[0], \
        "This hook should only be used with a FindConnection command"
    command_body = get_command_body(query[0])
    constraints = command_body.get("constraints", {})
    src_constraint = constraints.get("_src")
    dst_constraint = constraints.get("_dst")
    other_constraints = {k: v for k, v in constraints.items()
                         if k not in ["_src", "_dst"]}
    list_columns = command_body.get("result", {}).get(
        "list", [])  # We never use all_properties, even for "*"
    other_columns = [c for c in list_columns if c not in ["_src", "_dst"]]

    if class_name and not is_system_class:
        command_body["with_class"] = class_name

    result_query = []

    src_command = dst_command = connection_command = None
    return_fn = None

    if src_constraint:  # cases 2, 4, 5, or 6
        src_command_name = "FindEntity" if src_class[
            0] != "_" else f"Find[{src_class[1:]}]"
        src_ref = len(result_query) + 1
        src_command = {
            src_command_name: {
                **({"with_class": src_class} if src_class[0] != "_" else {}),
                "constraints": {"_uniqueid": src_constraint},
                "_ref": src_ref
            }
        }
        src_command_body = get_command_body(src_command)
        result_query.append(src_command)

    if dst_constraint:  # cases 3, 4, 5 or 6
        dst_command_name = "FindEntity" if dst_class[
            0] != "_" else f"Find[{dst_class[1:]}]"
        dst_ref = len(result_query) + 1
        dst_command = {
            dst_command_name: {
                **({"with_class": dst_class} if dst_class[0] != "_" else {}),
                "constraints": {"_uniqueid": dst_constraint},
                "_ref": dst_ref
            }
        }
        dst_command_body = get_command_body(dst_command)
        result_query.append(dst_command)

    # cases 1, 2, 3, or 4
    if other_columns or other_constraints or not (src_command and dst_command):
        assert not is_system_class, \
            "Cannot use FindConnection with other constraints or columns on a system class"
        command_body["constraints"] = other_constraints
        if src_command is not None:
            command_body["src"] = src_command
        if dst_command is not None:
            command_body["dst"] = dst_command

        connection_command = query[0].copy()
        if src_command:
            connection_command["FindConnection"]["src"] = src_ref
        if dst_command:
            connection_command["FindConnection"]["dst"] = dst_ref
        if other_constraints:
            connection_command["FindConnection"]["constraints"] = other_constraints
        else:
            del connection_command["FindConnection"]["constraints"]
        result_query.append(connection_command)

        if src_command:
            if dst_command:
                logger.debug(
                    f"connection table case 4: A {src_command_name}{'(class='+src_class+')' if src_class[0] != '_' else ''} command ref-tied to `src` and a {dst_command_name}{'(class='+dst_class+')' if dst_class[0] != '_' else ''} command ref-tied to `dst` in FindConnection{'(class='+class_name+')' if class_name else ''}")
            else:
                logger.debug(
                    f"connection table case 2: A {src_command_name}{'(class='+src_class+')' if src_class[0] != '_' else ''} command ref-tied to `src` in FindConnection{'(class='+class_name+')' if class_name else ''}")
        elif dst_command:
            logger.debug(
                f"connection table case 3: A {dst_command_name}{'(class='+dst_class+')' if dst_class[0] != '_' else ''} command ref-tied to `dst` in FindConnection{'(class='+class_name+')' if class_name else ''}")
        else:
            logger.debug(
                f"connection table case 1: Single FindConnection{'(class='+class_name+')' if class_name else ''} command")

    else:  # 5 or 6, no FindConnection command; add is_connected_to
        def get_result_objects(response: List[dict], direction: Literal["in", "out"]) -> Iterable[dict]:
            """
            Extracts the result objects from the response.
            This is used to rewrite the results for cases 5 and 6.
            """
            assert isinstance(response, list) and len(response) == 2, \
                f"Response should have exactly two elements for is_connected_to: {response}"
            d = get_command_body(response[-1]).get("entities", {})
            for k, v in d.items():
                for x in v:
                    if direction == "out":
                        yield {"_src": k, "_dst": x["_uniqueid"]}
                    else:
                        yield {"_src": x["_uniqueid"], "_dst": k}

        assert src_command and dst_command, \
            "Cannot drop FindConnection without both src and dst commands"
        classes = get_classes("entities")
        src_count = classes.get(src_class, {}).get("matched", 0)
        dst_count = classes.get(dst_class, {}).get("matched", 0)

        if src_count < dst_count:  # case 5
            logger.debug(
                f"connection table case 5: A {src_command_name}{'(class='+src_class+')' if src_class[0] != '_' else ''} command, and a {dst_command_name}{'(class='+dst_class+')' if dst_class[0] != '_' else ''} command with is_connected_to(direction=out{', class='+class_name if class_name else ''})")
            dst_command_body["is_connected_to"] = {
                **({"connection_class": class_name} if class_name else {}),
                "direction": "out",
                "ref": src_ref
            }
            dst_command_body["group_by_source"] = True
            if "batch" in command_body:
                # Cannot use batch with group_by_source
                dst_command_body["limit"] = command_body["batch"]["batch_size"]
            del dst_command_body["_ref"]
            if not get_command_body(result_query[-1]).get("results", {}):
                # If the original query did not specify a list, we default to _uniqueid
                get_command_body(
                    result_query[-1])["results"] = {"list": ["_uniqueid"]}

            def return_fn(r): return get_result_objects(r, "out")
        else:  # case 6
            logger.debug(
                f"connection table case 6: A {dst_command_name}{'(class='+dst_class+')' if dst_class[0] != '_' else ''} command, and a {src_command_name}{'(class='+src_class+')' if src_class[0] != '_' else ''} command with is_connected_to(direction='in'{', class='+class_name if class_name else ''})")
            src_command_body["is_connected_to"] = {
                **({"connection_class": class_name} if class_name else {}),
                "direction": "in",
                "ref": dst_ref
            }
            src_command_body["group_by_source"] = True
            if "batch" in command_body:
                # Cannot use batch with group_by_source
                src_command_body["limit"] = command_body["batch"]["batch_size"]
            del src_command_body["_ref"]
            result_query.reverse()  # Reverse the order so it is [dst, src]
            if not get_command_body(result_query[-1]).get("results", {}):
                # If the original query did not specify a list, we default to _uniqueid
                get_command_body(
                    result_query[-1])["results"] = {"list": ["_uniqueid"]}

            def return_fn(r): return get_result_objects(r, "in")

    # Replace query with the result query
    query.clear()
    query.extend(result_query)
    return return_fn
