from dotenv import load_dotenv
from typing import Optional, Set, Tuple, Generator, List, Dict
from itertools import zip_longest
from multicorn import TableDefinition, ColumnDefinition, ForeignDataWrapper
import sys
from datetime import datetime
from aperturedb.CommonLibrary import create_connector
import logging
import os
import json

# Configure logging
logging.basicConfig(level=logging.INFO, force=True)
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

# Queries are processed in batches, but the client doesn't know because result rows are yielded one by one.
BATCH_SIZE = 100
BATCH_SIZE_WITH_BLOBS = 10


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
        """
        Normalize a row to ensure it has the correct types for PostgreSQL.
        This is used to convert ApertureDB types to PostgreSQL types.
        """
        result = {}
        for col in columns:
            if col not in row:
                continue
            type_ = self._columns[col]["type"]
            if type_ == "datetime":
                value = row[col]["_date"] if row[col] else None
            elif type_ == "json":
                value = json.dumps(row[col])
            elif type_ == "blob":
                value = row[col]
            else:
                value = row[col]
            result[col] = value
        return result

    def _get_as_format(self, quals) -> Optional[str]:
        """
        Get the 'as_format' from the quals if it exists.
        This is used to determine how to return image data.
        """
        for qual in quals:
            if qual.field_name == "_as_format":
                assert qual.operator == "=", f"Unexpected operator for _as_format: {qual.operator}  Expected '='"
                return qual.value
        return None

    def _get_operations(self, quals) -> Optional[List[dict]]:
        """
        Get the 'operations' from the quals if it exists.
        This is used to determine what operations to perform on the image data.
        """
        for qual in quals:
            if qual.field_name == "_operations":
                assert qual.operator == "=", f"Unexpected operator for _operations: {qual.operator}  Expected '='"
                return json.loads(qual.value)
        return None

    def _get_blob_columns(self, columns: Set[str]) -> Tuple[Optional[str], Set[str]]:
        """
        Get the first blob column from the columns set, and the remaining columns.
        """
        blob_columns = [
            col for col in columns if self._columns[col]["type"] == "blob"]
        if len(blob_columns) > 1:
            logger.warning(
                f"Multiple blob columns requested: {blob_columns}. Only the first will be returned.")
        blob_column = blob_columns[0] if blob_columns else None
        filtered_columns = {
            col for col in columns if col not in blob_columns and not self._columns[col].get("special", False)}
        return blob_column, set(filtered_columns)

    def _get_query(self, columns: Set[str], blobs: bool, as_format: Optional[str], operations: Optional[List[dict]], batch_size: int) -> List[dict]:
        """
        Construct the query to execute against ApertureDB.
        This is used to build the query based on the columns and options.
        """
        query = [{
            self._command: {
                **({"with_class": self._class} if not self._is_system_class else {}),
                **({"results": {"list": list(columns)}} if columns else {}),
                "batch": {
                    "batch_id": 0,
                    "batch_size": batch_size
                },
                **({"blobs": True} if blobs else {}),
                **({"as_format": as_format} if as_format else {}),
                **({"operations": operations} if operations else {}),
            }
        }]
        return query

    def _get_next_query(self, query: List[dict], response: List[dict]) -> Optional[List[dict]]:
        """
        Get the next query to execute based on the response from the previous query.
        This is used to handle batching.
        """
        if not response or len(response) != 1:
            logger.warning(
                f"No results found for query: {query} -> {response}")
            return None

        if "batch" not in response[0][self._command]:
            # Some commands (like FindConnection) don't handle batching, so we assume all results are returned at once.
            logger.info(
                f"Single batch found for query: {query} -> {response[:10]}")
            return None

        batch_id = response[0][self._command]["batch"]["batch_id"]
        total_elements = response[0][self._command]["batch"]["total_elements"]
        end = response[0][self._command]["batch"]["end"]

        if end >= total_elements:  # No more batches to process
            return None

        next_query = query.copy()
        next_query[0][self._command]["batch"]["batch_id"] += 1
        return next_query

    def _get_query_results(self, query: List[dict]) -> Generator[Tuple[dict, Optional[bytes]], None, List[dict]]:
        logger.debug(f"Executing query: {query}")

        start_time = datetime.now()
        _, results, blobs = POOL.execute_query(query)
        elapsed_time = datetime.now() - start_time
        logger.info(
            f"Query executed in {elapsed_time.total_seconds()} seconds. Results: {results}, Blobs: {len(blobs) if blobs else 0}")

        if not results or len(results) != 1:
            logger.warning(
                f"No results found for entity query. {query} -> {results}")
            raise ValueError(
                f"No results found for entity query: {query}. Please check the class and columns. Results: {results}")

        result_objects = results[0].get(
            self._command, {}).get(self._result_field, [])
        if not blobs:
            for row in result_objects:
                yield row, None
        else:
            for row, blob in zip_longest(result_objects, blobs):
                yield row or {}, blob

        return results

    def execute(self, quals, columns):
        """ Execute the FDW query with the given quals and columns.

        Args:
            quals (list): List of conditions to filter the results.
                Note that filtering is optional because PostgreSQL will also filter the results.
            columns (set): List of columns to return in the results.
        """

        logger.info(
            f"Executing FDW {self._type}/{self._class} with quals: {quals} and columns: {columns}")

        blob_column, filtered_columns = self._get_blob_columns(columns)
        batch_size = BATCH_SIZE_WITH_BLOBS if blob_column else BATCH_SIZE
        as_format = self._get_as_format(quals)
        operations = self._get_operations(quals)

        query = self._get_query(
            columns=filtered_columns,
            blobs=bool(blob_column),
            as_format=as_format,
            operations=operations,
            batch_size=batch_size)

        n_results = 0
        while query:
            gen = self._get_query_results(query)
            try:
                while True:
                    row, blob = next(gen)

                    # Add blob to the row if it exists
                    if blob_column:
                        row[blob_column] = blob

                    # Add special columns if they exist so that Postgres doesn't filter out rows
                    if as_format:
                        row["_as_format"] = as_format
                    if operations:
                        row["_operations"] = operations

                    result = self._normalize_row(columns, row)

                    logger.debug(
                        f"Yielding row: {json.dumps({k: v[:10] if isinstance(v, str) else len(v) if isinstance(v, (bytes, list)) else v for k, v in row.items()}, indent=2)}"
                    )
                    n_results += 1
                    if n_results % 1000 == 0:
                        logger.info(
                            f"Yielded {n_results} results so far for FDW {self._type}/{self._class}")
                    yield result
            except StopIteration as e:
                response = e.value  # return value from _get_query_results

            query = self._get_next_query(query, response)
        logger.info(
            f"Executed FDW {self._type}/{self._class} with {n_results} results")

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
    def _entity_table(cls, entity: str, data: dict) -> TableDefinition:
        """
        Create a TableDefinition for an entity.
        This is used to create the foreign table in PostgreSQL.
        """
        columns = []

        try:
            if data["properties"] is not None:
                for prop, prop_data in data["properties"].items():
                    try:
                        count, indexed, type_ = prop_data
                        columns.append(ColumnDefinition(
                            column_name=prop, type_name=TYPE_MAP[type_.lower()], options=cls._encode_options({"count": count, "indexed": indexed, "type": type_.lower()})))
                    except Exception as e:
                        logger.exception(
                            f"Error processing property '{prop}' for entity {entity}: {e}")
                        raise

            # Add the _uniqueid column
            columns.append(ColumnDefinition(
                column_name="_uniqueid", type_name="text", options=cls._encode_options({"count": data["matched"], "indexed": True, "unique": True, "type": "string"})))

            # Blob-like entities get special columns specific to the type
            if entity == "_Blob":
                # _Blob gets _blob column
                columns.append(ColumnDefinition(
                    column_name="_blob", type_name="bytea",
                    options=cls._encode_options({"count": data["matched"], "indexed": False, "type": "blob", "special": True})))
            elif entity == "_Image":
                # _image gets _image, _as_format, _operations columns
                columns.append(ColumnDefinition(
                    column_name="_image", type_name="bytea",
                    options=cls._encode_options({"count": data["matched"], "indexed": False, "type": "blob", "special": True})))
                columns.append(ColumnDefinition(
                    column_name="_as_format", type_name="image_format_enum",
                    options=cls._encode_options({"count": data["matched"], "indexed": False, "type": "string", "special": True})))
                columns.append(ColumnDefinition(
                    column_name="_operations", type_name="jsonb",
                    options=cls._encode_options({"count": data["matched"], "indexed": False, "type": "json", "special": True})))
        except Exception as e:
            logger.exception(
                f"Error processing properties for entity {entity}: {e}")
            raise

        options = {
            "class": entity,
            "type": "entity",
            "matched": data["matched"],
        }

        logger.debug(
            f"Creating entity table for {entity} with columns: {columns} and options: {options}")

        return TableDefinition(
            table_name=entity,
            columns=columns,
            options=cls._encode_options(options)
        )

    @classmethod
    def _connection_table(cls, connection: str, data: dict) -> TableDefinition:
        """
        Create a TableDefinition for a connection.
        This is used to create the foreign table in PostgreSQL.
        """
        columns = []

        try:
            if data["properties"] is not None:
                for prop, prop_data in data["properties"].items():
                    try:
                        count, indexed, type_ = prop_data
                        columns.append(ColumnDefinition(
                            column_name=prop, type_name=TYPE_MAP[type_.lower()], options=cls._encode_options({"count": count, "indexed": indexed, "type": type_.lower()})))

                    except Exception as e:
                        logger.exception(
                            f"Error processing property '{prop}' for connection {connection}: {e}")
                        raise

            # Add the _uniqueid, _src, and _dst columns
            columns.append(ColumnDefinition(
                column_name="_uniqueid", type_name="text", options=cls._encode_options({"count": data["matched"], "indexed": True, "unique": True, "type": "string"})))
            columns.append(ColumnDefinition(
                column_name="_src", type_name="text", options=cls._encode_options({"class": data["src"], "count": data["matched"], "indexed": True, "type": "string"})))
            columns.append(ColumnDefinition(
                column_name="_dst", type_name="text", options=cls._encode_options({"class": data["dst"], "count": data["matched"], "indexed": True, "type": "string"})))
        except Exception as e:
            logger.exception(
                f"Error processing properties for connection {connection}: {e}")
            raise

        options = {
            "class": connection,
            "type": "connection",
            "matched": data["matched"],
        }

        logger.debug(
            f"Creating connection table for {connection} with columns: {columns} and options: {options}")

        return TableDefinition(
            table_name=connection,
            columns=columns,
            options=cls._encode_options(options)
        )

    @classmethod
    def import_schema(cls, schema, srv_options, options, restriction_type, restricts):
        """
        Import the schema from ApertureDB and return a list of TableDefinitions.
        This method is called when the foreign data wrapper is created.
        The result of this is to create the foreign tables in PostgreSQL.

        Note that we cannot add comments, foreign keys, or other constraints here.
        """
        logger.info("Importing schema with options: %s", options)
        results = []
        if "entities" in SCHEMA and "classes" in SCHEMA["entities"]:
            for entity, data in SCHEMA["entities"]["classes"].items():
                results.append(cls._entity_table(entity, data))

        if "connections" in SCHEMA and "classes" in SCHEMA["connections"]:
            for connection, data in SCHEMA["connections"]["classes"].items():
                results.append(cls._connection_table(connection, data))
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
