from multicorn import TableDefinition, ColumnDefinition, ForeignDataWrapper
import sys
from datetime import datetime
from aperturedb.CommonLibrary import create_connector
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, force=True,)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler("/tmp/fdw.log")
handler.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)
logger.propagate = False


def import_from_app():
    try:
        sys.path.append('/app')
        from connection_pool import ConnectionPool
    except ImportError as e:
        logger.exception("Failed to import ConnectionPool from app: %s", e)
        sys.exit(1)


def main():
    try:
        import_from_app()
        logger.info(
            f"APERTUREB_KEY={os.environ.get('APERTUREB_KEY', 'Not Set')}")
        db = create_connector()
        # global pool
        # pool = ConnectionPool(db.clone)
        # global schema
        # with pool.get_utils() as utils:
        #     schema = utils.get_schema()
    except Exception as e:
        logger.exception("Error during initialization: %s", e)
        sys.exit(1)


main()

TYPE_MAP = {
    "number": "double precision",
    "string": "text",
    "boolean": "boolean",
    "datetime": "timestamptz"
}


class FDW(ForeignDataWrapper):
    def execute(self, quals, columns):
        print("Executing FDW with quals:", quals, "and columns:", columns)
        return []

    @classmethod
    def import_schema(cls, schema, srv_options, options, restriction_type, restricts):
        results = []
        # if "entities" in schema and "classes" in schema["entities"]:
        #     for entity, data in schema["entities"]["classes"].items():
        #         columns = []
        #         for prop, prop_data in entity["properties"].items():
        #             count, indexed, type_ = prop_data
        #             columns.append(ColumnDefinition(
        #                 column_name=prop, type_name=TYPE_MAP[type_.lower()]))
        #         columns.append(ColumnDefinition(
        #             column_name="_unique_id", type_name="text"))
        #     results.append(TableDefinition(
        #         entity,
        #         columns=columns,
        #     ))

        # if "connections" in schema and "classes" in schema["connections"]:
        #     for connection, data in schema["connections"]["classes"].items():
        #         columns = []
        #         for prop, prop_data in connection["properties"].items():
        #             count, indexed, type_ = prop_data
        #             columns.append(ColumnDefinition(
        #                 column_name=prop, type_name=TYPE_MAP[type_.lower()]))
        #         columns.append(ColumnDefinition(
        #             column_name="_unique_id", type_name="text"))
        #         columns.append(ColumnDefinition(
        #             column_name="_src", type_name="text"))
        #         columns.append(ColumnDefinition(
        #             column_name="_dst", type_name="text"))
        #         results.append(TableDefinition(
        #             connection,
        #             columns=columns,
        #         ))

        return results


print("FDW class defined successfully")
