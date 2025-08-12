from typing import Callable, Any
from pydoc import locate
from pydantic import BaseModel, GetCoreSchemaHandler, GetJsonSchemaHandler, TypeAdapter
from collections import defaultdict
from typing import List, Optional, Dict, Callable, Any, Literal
from multicorn import ColumnDefinition
from dotenv import load_dotenv
import logging
import sys
import os
import json
from contextlib import contextmanager
from pydantic_core import core_schema
import inspect

logger = logging.getLogger(__name__)


@contextmanager
def import_path(path):
    original = list(sys.path)
    sys.path.insert(0, path)
    try:
        yield
    finally:
        sys.path = original


@contextmanager
def import_from_app():
    """We don't control the import path, so we need to ensure the app directory is in the path."""
    with import_path('/app'):
        yield


def load_aperturedb_env(path="/app/aperturedb.env"):
    """Load environment variables from a file.
    This is used because FDW is executed in a "secure" environment where
    environment variables cannot be set directly.
    """
    if not os.path.exists(path):
        raise RuntimeError(f"Missing environment file: {path}")
    load_dotenv(dotenv_path=path, override=True)


_POOL = None  # Global connection pool; see get_pool()
_SCHEMA = None  # Global schema variable; see get_schema()


def get_log_level() -> int:
    """Get the log level from the environment variable."""
    load_aperturedb_env()
    log_level = os.getenv("WF_LOG_LEVEL", "WARN").upper()
    return getattr(logging, log_level, logging.WARN)


def get_pool() -> "ConnectionPool":
    """Get the global connection pool. Lazy initialization."""
    load_aperturedb_env()
    global _POOL
    if _POOL is None:
        with import_from_app():
            from connection_pool import ConnectionPool
        _POOL = ConnectionPool()
        logger.info("Connection pool initialized")
        with _POOL.get_connection() as c:
            logger.info(f"connection host = {c.host}")
    return _POOL


def get_schema() -> Dict:
    """Get the global schema. Lazy initialization."""
    global _SCHEMA
    if _SCHEMA is None:
        with get_pool().get_utils() as utils:
            _SCHEMA = utils.get_schema()
            logger.info("Schema loaded")
    return _SCHEMA


def get_classes(field: Literal["entities", "connections"],
                schema: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Get the classes for entities or connections from the schema.

    Guarantees to return a (possibly empty) dict, even if the field is not present.
    """
    if schema is None:
        schema = get_schema()
    if schema.get(field) is None:
        logger.warning(f"No {field} found in schema")
        return {}
    classes = schema.get(field, {}).get("classes", {})
    if not classes:
        logger.warning(f"No {field} classes found in schema")
        return {}
    return classes


# Mapping from ApertureDB types to PostgreSQL types.
TYPE_MAP = {
    "number": "double precision",
    "string": "text",
    "boolean": "boolean",
    "datetime": "timestamptz",
    "json": "jsonb",
    "blob": "bytea",
    # Not a real type, but used for _uniqueid, _src, and _dst columns because they have constraint operator restrictions.
    "uniqueid": "text",
}


logger = logging.getLogger(__name__)


class Curry:
    """
    This class is used to wrap functions so that they can be serialized and deserialized as text.
    The function is stored as a reference to its module and qualified name, 
    which basically means that it has to be a named top-level function in a module.
    Positional and keyword arguments can be supplied to give the effect of a function closure.
    These must be JSON-serializable.

    The reason for all this is that we want to be able to store functions in TableOptions and ColumnOptions,
    in order to specialize the behaviour of the `execute` method,
    but those datastructures are serialized to JSON in `import_schema`,
    because Postgres stores them in the database as text fields,
    and then later passes them to `FDW.__init__`, which deserializes them back into Python objects.
    """

    def __init__(self, func: Callable, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

        if not hasattr(func, "__module__") or not hasattr(func, "__qualname__"):
            raise TypeError(
                f"Expected a function or method with __module__ and __qualname__, "
                f"got object of type {type(func)}: {repr(func)}"
            )

    def validate_signature(self, required_keywords: set):
        sig = inspect.signature(self.func)
        param_names = set(sig.parameters.keys())
        accepts_kwargs = any(
            p.kind == inspect.Parameter.VAR_KEYWORD
            for p in sig.parameters.values()
        )

        if not accepts_kwargs and not required_keywords <= param_names:
            missing = required_keywords - param_names
            raise TypeError(
                f"Function {self.func} missing required keywords: {missing}")

        overlap = required_keywords & self.kwargs.keys()
        if overlap:
            raise TypeError(
                f"Curry for {self.func} should not override required args: {overlap}")

    def __call__(self, **kwargs):
        return self.func(*self.args, **self.kwargs, **kwargs)

    def to_json(self):
        return {
            "__curry__": True,
            "module": self.func.__module__,
            "qualname": self.func.__qualname__,
            "args": self.args,
            "kwargs": self.kwargs,
        }

    @classmethod
    def from_json(cls, data: dict):
        if not data.get("__curry__"):
            raise ValueError("Invalid Curry JSON data")
        module = data["module"]
        qualname = data["qualname"]
        args = data.get("args", [])
        kwargs = data.get("kwargs", {})
        func = locate(f"{module}.{qualname}")
        if func is None:
            raise ValueError(f"Could not locate function {module}.{qualname}")
        return cls(func, *args, **kwargs)

    @classmethod
    def _validate(cls, value: Any) -> "Curry":
        logger.debug(f"Validating Curry: {value}")
        if isinstance(value, cls):
            return value
        elif isinstance(value, dict):
            return cls.from_json(value)
        raise TypeError(f"Cannot convert {value!r} to Curry")

    @classmethod
    def _serialize(cls, value: "Curry") -> dict:
        return value.to_json()

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type, handler):
        return core_schema.no_info_after_validator_function(
            cls._validate,
            core_schema.any_schema(),
            serialization=core_schema.plain_serializer_function_ser_schema(
                cls._serialize, when_used="always"
            )
        )

    def __repr__(self):
        return f"Curry({self.func.__module__}.{self.func.__qualname__}, args={self.args}, kwargs={self.kwargs})"


def compact_pretty_json(data: Any, line_length=78, level=0, indent=2) -> str:
    """
    Compact yet pretty JSON representation of the data.
    If it fits in one line (accounting for indentation), it stays one line.
    Otherwise, falls back to a multi-line indented format.
    """
    prefix = " " * (level * indent)
    one_line = json.dumps(data, ensure_ascii=False)

    if len(prefix) + len(one_line) <= line_length:
        return prefix + one_line

    if isinstance(data, (list, tuple)):
        lines = [prefix + "["]
        for item in data:
            lines.append(compact_pretty_json(
                item, line_length, level + 1, indent))
        lines.append(prefix + "]")
        return "\n".join(lines)

    elif isinstance(data, dict):
        lines = [prefix + "{"]
        for i, (key, value) in enumerate(data.items()):
            key_str = json.dumps(key, ensure_ascii=False) + ": "
            val_str = compact_pretty_json(
                value, line_length, level + 1, indent)
            lines.append(" " * ((level + 1) * indent) +
                         key_str + val_str.lstrip() + 
                         ("," if i + 1 < len(data) else ""))
        lines.append(prefix + "}")
        return "\n".join(lines)

    else:
        return prefix + json.dumps(data, ensure_ascii=False)
