from dataclasses import dataclass
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
import http.client
import socket

logger = logging.getLogger(__name__)


def load_aperturedb_env(path="/app/aperturedb.env"):
    """Load environment variables from a file.
    This is used because FDW is executed in a "secure" environment where
    environment variables cannot be set directly.
    """
    if not os.path.exists(path):
        raise RuntimeError(f"Missing environment file: {path}")
    load_dotenv(dotenv_path=path, override=True)


def get_log_level() -> int:
    """Get the log level from the environment variable."""
    load_aperturedb_env()
    sys.path.insert(0, '/app')
    from wf_argparse import validate
    log_level = validate("log_level", envar="WF_LOG_LEVEL", default="WARNING")
    return getattr(logging, log_level, logging.WARNING)


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
        for i, item in enumerate(data):
            lines.append(compact_pretty_json(
                item, line_length, level + 1, indent) +
                ("," if i != len(data) - 1 else ""))
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


def get_command_body(command: dict) -> dict:
    """
    Extract the command body from a command dictionary.
    This is used to get the command body for modify_query hooks.
    """
    return next(iter(command.values()))


@dataclass
class PathKey:
    """
    Represents a path key for a foreign table column.
    """
    columns: List[str]  # The path keys for the column
    expected_rows: int  # The expected number of rows for this path key


PROXY_SOCKET_PATH = "/tmp/aperturedb-proxy.sock"


class UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, uds_path: str):
        super().__init__("localhost")  # host is ignored
        self.uds_path = uds_path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(self.uds_path)
