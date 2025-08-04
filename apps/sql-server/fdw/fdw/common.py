from typing import Callable, Any
from pydoc import locate
from pydantic import BaseModel, GetCoreSchemaHandler, GetJsonSchemaHandler, TypeAdapter
from collections import defaultdict
from typing import List, Optional, Dict, Callable, Any
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
    return _POOL


def get_schema() -> Dict:
    """Get the global schema. Lazy initialization."""
    global _SCHEMA
    if _SCHEMA is None:
        with get_pool().get_utils() as utils:
            _SCHEMA = utils.get_schema()
            logger.info("Schema loaded")
    return _SCHEMA


# Mapping from ApertureDB types to PostgreSQL types.
TYPE_MAP = {
    "number": "double precision",
    "string": "text",
    "boolean": "boolean",
    "datetime": "timestamptz",
    "json": "jsonb",
    "blob": "bytea",
}


logger = logging.getLogger(__name__)


class Curry:
    def __init__(self, func: Callable, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

        if not hasattr(func, "__module__") or not hasattr(func, "__qualname__"):
            raise TypeError("Function must have __module__ and __qualname__")

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
