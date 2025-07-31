import json
import os
import sys
import logging
from dotenv import load_dotenv
from multicorn import ColumnDefinition
from typing import List

logger = logging.getLogger(__name__)


def load_aperturedb_env(path="/app/aperturedb.env"):
    """Load environment variables from a file.
    This is used because FDW is executed in a "secure" environment where
    environment variables cannot be set directly.
    """
    if not os.path.exists(path):
        raise RuntimeError(f"Missing environment file: {path}")
    load_dotenv(dotenv_path=path, override=True)


def main():
    try:
        load_aperturedb_env()
        sys.path.append('/app')
        from connection_pool import ConnectionPool
        global POOL
        POOL = ConnectionPool()
        global SCHEMA
        with POOL.get_utils() as utils:
            SCHEMA = utils.get_schema()
        logger.info(
            f"ApertureDB schema loaded successfully. \n{json.dumps(SCHEMA, indent=2)}")
    except Exception as e:
        logger.exception("Error during initialization: %s", e)
        sys.exit(1)


main()

# Mapping from ApertureDB types to PostgreSQL types.
TYPE_MAP = {
    "number": "double precision",
    "string": "text",
    "boolean": "boolean",
    "datetime": "timestamptz",
    "json": "jsonb",
    "blob": "bytea",
}


def encode_options(options):
    """
    Convert options so that all values are strings.
    Although PostgreSQL does nothing with these options, the value type must be string.
    """
    return {"fdw_config": json.dumps(options, default=str)}


def decode_options(options):
    """
    Convert options from a string back to a dictionary.
    """
    if not options:
        return {}
    try:
        return json.loads(options["fdw_config"])
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode options: {e}")
        return {}


def property_columns(data: dict) -> List[ColumnDefinition]:
    """
    Create a list of ColumnDefinitions for the given properties.
    This is used to create the foreign table in PostgreSQL.
    """
    columns = []
    if data["properties"] is not None:
        for prop, prop_data in data["properties"].items():
            try:
                count, indexed, type_ = prop_data
                columns.append(ColumnDefinition(
                    column_name=prop,
                    type_name=TYPE_MAP[type_.lower()],
                    options=encode_options({"count": count, "indexed": indexed, "type": type_.lower()})))
            except Exception as e:
                logger.exception(
                    f"Error processing property '{prop}': {e}")
                raise

    # Add the _uniqueid column
    columns.append(ColumnDefinition(
        column_name="_uniqueid", type_name="text", options=encode_options({"count": data["matched"], "indexed": True, "unique": True, "type": "string"})))

    return columns
