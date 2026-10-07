"""Convert FDW planner predicates to ApertureDB wire expressions.

Keep this module independent of the SDK: Multicorn runs in system Python,
which does not have the workflow virtual environment's SDK installed.
"""

from typing import Any, Optional


def literal(value: Any) -> Any:
    """
    Convert a value into an expression-array literal.

    Strings starting with ``$`` are escaped so they are not read as property
    references, ``_date`` wrappers become ``to_date`` calls, and lists become
    ``array`` constructors.
    """
    if isinstance(value, str) and value.startswith("$"):
        return "$" + value
    if isinstance(value, dict) and len(value) == 1 and ("_date" in value or "date" in value):
        return {"to_date": literal(next(iter(value.values())))}
    if isinstance(value, (list, tuple)):
        return {"array": [literal(v) for v in value]}
    if isinstance(value, dict):
        return {"json": value}
    return value


def predicate(key: str, op: str, value: Any) -> list:
    """Build a single ``["$key", op, value]`` comparison."""
    return ["$" + key, {"=!": "!=", "not in": "not_in"}.get(op, op), literal(value)]


def to_expression(constraints) -> Optional[list]:
    """
    Convert property-keyed constraints into an expression array.

    ``{"age": [">=", 20, "<=", 90], "name": ["==", "x"]}`` becomes
    ``["all", ["$age", ">=", 20], ["$age", "<=", 90], ["$name", "==", "x"]]``.
    Nested ``all``/``any`` groups are converted recursively. Expression arrays
    are returned unchanged, and empty constraints return None.
    """
    if constraints is None or isinstance(constraints, list):
        return constraints
    return _to_expression_group(constraints, "all")


def _to_expression_group(constraints: dict, conjunction: str) -> Optional[list]:
    terms = []
    for key, ops in constraints.items():
        if key in ("all", "any") and isinstance(ops, dict):
            group = _to_expression_group(ops, key)
            if group is not None:
                terms.append(group)
        else:
            comparisons = [predicate(key, ops[i], ops[i + 1])
                           for i in range(0, len(ops), 2)]
            # Both bounds of a property range must hold, even within an OR.
            if conjunction == "any" and len(comparisons) > 1:
                terms.append(["all"] + comparisons)
            else:
                terms.extend(comparisons)
    if not terms:
        return None
    if len(terms) == 1:
        return terms[0]
    return [conjunction] + terms


def query_expressions(query):
    """Copy command filters without interpreting properties or JSON payloads.

    Planning and connection rewrites use property-keyed predicates internally;
    only the request sent to the database uses expression arrays.
    """
    def links(value):
        if isinstance(value, list):
            return [links(item) for item in value]
        if isinstance(value, dict):
            result = filters(value)
            for group in ("all", "any"):
                if group in result:
                    result[group] = links(result[group])
            return result
        return value

    def filters(body):
        result = dict(body)
        for key in ("constraints", "if_not_found"):
            if key in result:
                expression = to_expression(result[key])
                if expression is None:
                    del result[key]
                else:
                    result[key] = expression
        for key in ("is_connected_to", "is_endpoint_of", "link", "connect"):
            if key in result:
                result[key] = links(result[key])
        return result

    return [{name: filters(body) for name, body in command.items()}
            for command in query]
