from multicorn import TableDefinition, ColumnDefinition, ForeignDataWrapper
import sys
from datetime import datetime
from aperturedb.CommonLibrary import create_connector
import logging
import os
import json

# Configure logging
logging.basicConfig(level=logging.INFO, force=True,)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler("/tmp/fdw.log")
handler.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)
logger.propagate = False


def load_aperturedb_env(path="/app/aperturedb.env"):
    if not os.path.exists(path):
        raise RuntimeError(f"Missing environment file: {path}")
    with open(path) as f:
        for line in f:
            if not line.strip() or line.startswith("#"):
                continue
            k, v = line.strip().split("=", 1)
            os.environ[k] = v
            logger.debug(f"Loaded environment variable: {k}")


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

TYPE_MAP = {
    "number": "double precision",
    "string": "text",
    "boolean": "boolean",
    "datetime": "timestamptz",
    "json": "jsonb",
}


class FDW(ForeignDataWrapper):
    def execute(self, quals, columns):
        print("Executing FDW with quals:", quals, "and columns:", columns)
        return []

    @classmethod
    def import_schema(cls, schema, srv_options, options, restriction_type, restricts):
        logger.info("Importing schema with options: %s", options)
        results = []
        if "entities" in SCHEMA and "classes" in SCHEMA["entities"]:
            for entity, data in SCHEMA["entities"]["classes"].items():
                columns = []
                if data["properties"] is not None:
                    for prop, prop_data in data["properties"].items():
                        count, indexed, type_ = prop_data
                        columns.append(ColumnDefinition(
                            column_name=prop, type_name=TYPE_MAP[type_.lower()]))
                columns.append(ColumnDefinition(
                    column_name="_unique_id", type_name="text"))
                logger.info(f"Adding entity {entity} with columns: {columns}")
                results.append(TableDefinition(
                    entity,
                    columns=columns,
                ))

        if "connections" in SCHEMA and "classes" in SCHEMA["connections"]:
            for connection, data in SCHEMA["connections"]["classes"].items():
                columns = []
                if data["properties"] is not None:
                    for prop, prop_data in data["properties"].items():
                        count, indexed, type_ = prop_data
                        columns.append(ColumnDefinition(
                            column_name=prop, type_name=TYPE_MAP[type_.lower()]))
                columns.append(ColumnDefinition(
                    column_name="_unique_id", type_name="text"))
                columns.append(ColumnDefinition(
                    column_name="_src", type_name="text"))
                columns.append(ColumnDefinition(
                    column_name="_dst", type_name="text"))
                logger.info(
                    f"Adding connection {connection} with columns: {columns}")
                results.append(TableDefinition(
                    connection,
                    columns=columns,
                ))

        return results


print("FDW class defined successfully")
