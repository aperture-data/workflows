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
    """Load environment variables from a file.
    This is used because FDW is executed in a "secure" environment where
    environment variables cannot be set directly.
    """
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

# Mapping from ApertureDB types to PostgreSQL types.
TYPE_MAP = {
    "number": "double precision",
    "string": "text",
    "boolean": "boolean",
    "datetime": "timestamptz",
    "json": "jsonb",
}

# Queries are processed in batches, but the client doesn't know because result rows are yielded one by one.
BATCH_SIZE = 100


class FDW(ForeignDataWrapper):
    """
    A Foreign Data Wrapper (FDW) for ApertureDB.
    This class allows PostgreSQL to interact with ApertureDB as if it were a foreign data source.
    Note that this class is instantiated once for each foreign table.
    The class method `import_schema` says what tables and columns to create in PostgreSQL.
    It also passes options for each table and column that are passed into `__init__`.
    """

    def __init__(self, fdw_options, fdw_columns):
        super().__init__(fdw_options, fdw_columns)
        self._options = self._decode_options(fdw_options)
        self._columns = {name: self._decode_options(
            col.options) for name, col in fdw_columns.items()}
        self._class = self._options["class"]
        self._type = self._options["type"]
        self._is_system_class = self._class[0] == "_"
        if self._type == "entity":
            self._command = self._get_command(self._class)
            self._result_field = "entities"
        elif self._type == "connection":
            self._command = "FindConnection"
            self._result_field = "connections"
        else:
            raise ValueError(f"Unknown type: {self._type}")
        logger.info("FDW initialized with options: %s", fdw_options)

    @staticmethod
    def _get_command(class_: str) -> str:
        """
        Get the command to use for the given class.
        This is used to determine how to query ApertureDB.
        """
        if class_[0] == "_":
            return f"Find{class_[1:]}"
        else:
            return "FindEntity"

    def _normalize_row(self, columns, row: dict) -> dict:
        result = {}
        for col in columns:
            if col not in row:
                continue
            type_ = self._columns[col]["type"]
            if type_ == "datetime":
                value = row[col]["_date"]
            elif type_ == "json":
                value = json.dumps(row[col])
            else:
                value = row[col]
            result[col] = value
        return result

    def execute(self, quals, columns):
        """ Execute the FDW query with the given quals and columns.

        Args:
            quals (list): List of conditions to filter the results.
                Note that filtering is optional because PostgreSQL will also filter the results.
            columns (set): List of columns to return in the results.
        """

        logger.info(
            f"Executing FDW {self._type}/{self._class} with quals: {quals} and columns: {columns}")

        # First we run a batch query to get the number of results.
        query = [
            {
                self._command: {
                    **({"with_class": self._class} if not self._is_system_class else {}),
                    **({"results": {"list": list(columns)}}),
                    "batch": {},
                }
            }
        ]

        _, results, _ = POOL.execute_query(query)

        if not results or len(results) != 1:
            logger.warning(
                f"No results found for entity query. {query} -> {results}")
            raise ValueError(
                f"No results found for entity query: {query}. Please check the class and columns. Results: {results}")

        if "batch" not in results[0][self._command]:
            # Some find commands (FindConnection) don't handle batching, so we assume all results are returned at once.
            logger.info(
                f"Single batch found for query: {query} -> {results[:10]}")
            rows = results[0][self._command][self._result_field]
            for row in rows:
                yield self._normalize_row(columns, row)
            return

        try:
            n_results = results[0][self._command]["batch"]["total_elements"]
        except KeyError:
            logger.error(
                f"Batch total_elements not found in results: {query} -> {results}")
            raise ValueError(
                f"Batch total_elements not found in results: {query} -> {results}")

        n_batches = (n_results + BATCH_SIZE - 1) // BATCH_SIZE
        logger.info(
            f"Found {n_results} results in {n_batches} batches for query: {query}")

        # Now we fetch the results batch by batch.
        for batch in range(n_batches):
            logger.info(
                f"Processing batch {batch + 1}/{n_batches}: {batch * BATCH_SIZE} to {min((batch + 1) * BATCH_SIZE, n_results)}")
            query[0][self._command]["batch"]["batch_id"] = batch
            query[0][self._command]["batch"]["batch_size"] = BATCH_SIZE
            _, results, _ = POOL.execute_query(query)
            if not results or len(results) != 1:
                logger.warning(
                    f"No results found for batch {batch} of entity query. {query} -> {results}")
                continue

            try:
                rows = results[0][self._command][self._result_field]
            except KeyError:
                logger.error(
                    f"Result field '{self._result_field}' not found in results: {query} -> {results}")
                raise ValueError(
                    f"Result field '{self._result_field}' not found in results: {query} -> {results}")

            if not rows:
                logger.warning(f"No rows found in batch {batch}. Continuing.")
                continue

            for row in rows:
                yield self._normalize_row(columns, row)

    @staticmethod
    def _encode_options(options):
        """
        Convert options so that all values are strings.
        Although PostgreSQL does nothing with these options, the value type must be string.
        """
        return {"fdw_config": json.dumps(options, default=str)}

    @staticmethod
    def _decode_options(options):
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

    @classmethod
    def import_schema(cls, schema, srv_options, options, restriction_type, restricts):
        """
        Import the schema from ApertureDB and return a list of TableDefinitions.
        This method is called when the foreign data wrapper is created.
        The result of this is to create the foreign tables in PostgreSQL.

        Note that we cannot
        """
        logger.info("Importing schema with options: %s", options)
        results = []
        if "entities" in SCHEMA and "classes" in SCHEMA["entities"]:
            for entity, data in SCHEMA["entities"]["classes"].items():
                columns = []
                if data["properties"] is not None:
                    for prop, prop_data in data["properties"].items():
                        count, indexed, type_ = prop_data
                        columns.append(ColumnDefinition(
                            column_name=prop, type_name=TYPE_MAP[type_.lower()], options=cls._encode_options({"count": count, "indexed": indexed, "type": type_.lower()})))
                # Add the _uniqueid column
                # This is a special column that is always present in entities, but does not appear in the schema.
                columns.append(ColumnDefinition(
                    column_name="_uniqueid", type_name="text", options=cls._encode_options({"count": data["matched"], "indexed": True, "unique": True, "type": "string"})))
                logger.info(f"Adding entity {entity} with columns: {columns}")
                results.append(TableDefinition(
                    table_name=entity,
                    columns=columns,
                    options=cls._encode_options({
                        "class": entity,
                        "type": "entity",
                        "matched": data["matched"],
                    })

                ))

        if "connections" in SCHEMA and "classes" in SCHEMA["connections"]:
            for connection, data in SCHEMA["connections"]["classes"].items():
                columns = []
                if data["properties"] is not None:
                    for prop, prop_data in data["properties"].items():
                        count, indexed, type_ = prop_data
                        columns.append(ColumnDefinition(
                            column_name=prop, type_name=TYPE_MAP[type_.lower()], options=cls._encode_options({"count": count, "indexed": indexed, "type": type_.lower()})))
                # Add the _uniqueid, _src, and _dst columns
                # These are special columns that are always present in connections, but do not appear in the schema.
                columns.append(ColumnDefinition(
                    column_name="_uniqueid", type_name="text", options=cls._encode_options({"count": data["matched"], "indexed": True, "unique": True, "type": "string"})))
                columns.append(ColumnDefinition(
                    column_name="_src", type_name="text", options=cls._encode_options({"class": data["src"], "count": data["matched"], "indexed": True, "type": "string"})))
                columns.append(ColumnDefinition(
                    column_name="_dst", type_name="text", options=cls._encode_options({"class": data["dst"], "count": data["matched"], "indexed": True, "type": "string"})))
                logger.info(
                    f"Adding connection {connection} with columns: {columns}")
                results.append(TableDefinition(
                    table_name=connection,
                    columns=columns,
                    options=cls._encode_options({
                        "class": connection,
                        "type": "connection",
                        "src": data["src"],
                        "dst": data["dst"],
                        "matched": data["matched"]
                    })
                ))

        return results


print("FDW class defined successfully")
