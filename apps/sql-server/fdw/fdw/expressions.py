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
