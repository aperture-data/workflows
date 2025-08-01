import json
import os
import sys
import logging
from dotenv import load_dotenv
from multicorn import ColumnDefinition
from typing import List, Optional, Dict
from collections import defaultdict
from pydantic import BaseModel

logger = logging.getLogger(__name__)


def load_aperturedb_env(path="/app/aperturedb.env"):
    """Load environment variables from a file.
    This is used because FDW is executed in a "secure" environment where
    environment variables cannot be set directly.
    """
    if not os.path.exists(path):
        raise RuntimeError(f"Missing environment file: {path}")
    load_dotenv(dotenv_path=path, override=True)


_POOL = None  # Global connection pool
_SCHEMA = None  # Global schema variable


def get_pool() -> "ConnectionPool":
    """Get the global connection pool. Lazy initialization."""
    load_aperturedb_env()
    sys.path.append('/app')
    from connection_pool import ConnectionPool
    global _POOL
    if _POOL is None:
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


def property_columns(data: dict) -> List[ColumnDefinition]:
    """
    Create a list of ColumnDefinitions for the given properties.
    This is used to create the foreign table in PostgreSQL.
    """
    columns = []
    if "properties" in data and data["properties"] is not None:
        assert isinstance(data["properties"], dict), \
            f"Expected properties to be a dict, got {type(data['properties'])}"
        for prop, prop_data in data["properties"].items():
            try:
                count, indexed, type_ = prop_data
                columns.append(ColumnDefinition(
                    column_name=prop,
                    type_name=TYPE_MAP[type_.lower()],
                    options=ColumnOptions(count=count, indexed=indexed, type=type_.lower()).to_string()))
            except Exception as e:
                logger.exception(
                    f"Error processing property '{prop}': {e}")
                raise

    # Add the _uniqueid column
    columns.append(ColumnDefinition(
        column_name="_uniqueid", type_name="text", options=ColumnOptions(count=data.get("matched", 0), indexed=True, unique=True, type="string").to_string()))

    return columns


class TableOptions(BaseModel):
    """
    Options passed to the foreign table from `import_schema`.
    """
    class_: Optional[str] = None  # class name of the entity or connection as reported by GetSchema
    type: str = "entity"  # object type, e.g. "entity", "connection", "descriptor"
    count: int = 0  # number of matched objects
    # command to execute, e.g. "FindEntity", "FindConnection", etc.
    command: str = "FindEntity"
    # field to look for in the response, e.g. "entities", "connections"
    result_field: str = "entities"
    extra: dict = {}  # additional options for the command
    blob_column: Optional[str] = None  # column containing blob data
    # properties of the descriptor set, if applicable
    descriptor_set_properties: Optional[dict] = None
    # operation types for the descriptor set, if applicable
    operation_types: Optional[List[str]] = None

    @classmethod
    def from_string(cls, options_str: Dict[str, str]) -> "TableOptions":
        """
        Create a TableOptions instance from a string dictionary.
        This is used to decode options from the foreign table definition.
        Postgres restricts options to be a string-valued dictionary.
        """
        options = json.loads(options_str["table_options"])
        return cls(**options)

    def to_string(self) -> Dict[str, str]:
        """
        Convert TableOptions to a string dictionary.
        This is used to encode options for the foreign table definition.
        """
        return {"table_options": json.dumps(self.dict(), default=str)}


class ColumnOptions(BaseModel):
    """
    Options passed to the foreign table columns from `import_schema`.
    """
    count: Optional[int] = None  # number of matched objects for this column
    indexed: bool = False  # whether the column is indexed
    type: str  # AQL type of the column: "string", "number", "boolean", "json", "blob"
    # whether the column has special meaning (e.g. _blob, _image)
    listable: bool = True  # whether the column can be passed to results/list
    unique: bool = False  # whether the column is unique, used for _uniqueid

    @classmethod
    def from_string(cls, options_str: Dict[str, str]) -> "ColumnOptions":
        """
        Create a ColumnOptions instance from a string dictionary.
        This is used to decode options from the foreign table column definition.
        """
        options = json.loads(options_str["column_options"])
        return cls(**options)

    def to_string(self) -> Dict[str, str]:
        """
        Convert ColumnOptions to a string dictionary.
        This is used to encode options for the foreign table column definition.
        """
        return {"column_options": json.dumps(self.dict(), default=str)}
