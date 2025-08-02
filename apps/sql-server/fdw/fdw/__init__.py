from .common import get_pool, get_log_level, import_from_app, TableOptions, ColumnOptions
from collections import defaultdict
from dotenv import load_dotenv
from typing import Optional, Set, Tuple, Generator, List, Dict
from itertools import zip_longest
from multicorn import TableDefinition, ColumnDefinition, ForeignDataWrapper, Qual
import sys
from datetime import datetime
from aperturedb.CommonLibrary import create_connector
import logging
import os
import json
import atexit
import numpy as np

with import_from_app():
    from embeddings import Embedder

# Configure logging
log_level = get_log_level()
handler = logging.FileHandler("/tmp/fdw.log", delay=False)
handler.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)s %(message)s"))
handler.setLevel(log_level)
handler.stream.flush = lambda: None  # Ensure flush is always available

logging.basicConfig(level=log_level, force=True)
logger = logging.getLogger(__name__)
logger.setLevel(log_level)
logger.addHandler(handler)
logger.propagate = False


def flush_logs():
    for h in logger.handlers:
        try:
            h.flush()
        except Exception:
            pass


atexit.register(flush_logs)


# Mapping from ApertureDB types to PostgreSQL types.
TYPE_MAP = {
    "number": "double precision",
    "string": "text",
    "boolean": "boolean",
    "datetime": "timestamptz",
    "json": "jsonb",
    "blob": "bytea"
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

        self._options = TableOptions.from_string(fdw_options)
        self._columns = {
            name: ColumnOptions.from_string(col.options)
            for name, col in fdw_columns.items()}
        logger.info("FDW initialized with options: %s", fdw_options)

    def _normalize_row(self, columns, row: dict) -> dict:
        """
        Normalize a row to ensure it has the correct types for PostgreSQL.
        This is used to convert ApertureDB types to PostgreSQL types.
        """
        result = {}
        for col in columns:
            if col not in row:
                continue
            type_ = self._columns[col].type
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
                operations = json.loads(qual.value)
                for op in operations:
                    if not isinstance(op, dict):
                        raise ValueError(
                            f"Invalid operation format: {op}. Expected a dictionary.")
                    if "type" not in op:
                        raise ValueError(
                            f"Operation must have 'type': {op}")
                    if op["type"] not in self._options.operation_types:
                        raise ValueError(
                            f"Invalid operation type: {op['type']}. Expected one of {self._options.operation_types}")
                return operations
        return None

    def _get_find_similar(self, quals) -> Tuple[bool, dict, Optional[bytes]]:
        """
        Check if the 'find_similar' option is set in the quals.
        This is used to determine if we should perform a 'find similar' query.

        Args:
            quals (list): List of conditions to filter the results.

        Returns:
            find_similar: Boolean indicating if 'find similar' is requested.
            extra: Dictionary with additional parameters for `FindDescriptor`
            blob: Optional bytes for the blob data if applicable.
        """
        if not self._options.find_similar:
            return False, {}, None

        if not quals:
            return False, {}, None

        for qual in quals:
            if qual.field_name == "_find_similar":
                assert qual.operator == "=", f"Unexpected operator for _find_similar: {qual.operator}  Expected '='"
                try:
                    find_similar = json.loads(qual.value)
                except json.JSONDecodeError as e:
                    raise ValueError(
                        f"Invalid JSON for _find_similar: {qual.value}") from e
                logger.debug(f"find_similar: {find_similar}")
                if not isinstance(find_similar, dict):
                    raise ValueError(
                        f"Invalid find_similar format: {find_similar}. Expected a dictionary.")
                extra = {k: v for k, v in find_similar.items() if k in [
                    "k_neighbors", "knn_first"] and v is not None}
                logger.debug(
                    f"find_similar extra parameters: {extra}")

                if "vector" in find_similar and find_similar["vector"] is not None:
                    vector = np.array(find_similar["vector"])
                    expected_size = self._options.descriptor_set_properties["_dimensions"]
                    if vector.shape != (expected_size,):
                        raise ValueError(
                            f"Invalid vector size: {vector.shape}. Expected {expected_size}.")
                else:
                    embedder = Embedder.from_properties(
                        properties=self._options.descriptor_set_properties,
                        descriptor_set=self._options.descriptor_set,
                    )

                    if "text" in find_similar and find_similar["text"] is not None:
                        text = find_similar["text"]
                        vector = embedder.embed_text(text)
                    elif "image" in find_similar and find_similar["image"] is not None:
                        image = find_similar["image"]
                        vector = embedder.embed_image(image)
                    else:
                        raise ValueError(
                            "find_similar must have one of 'text', 'image', or 'vector' to embed.")

                return True, extra, vector.tobytes()

        return False, {}, None

    def _get_query(self,
                   columns: Set[str],
                   blobs: bool,
                   as_format: Optional[str],
                   operations: Optional[List[dict]],
                   batch_size: int,
                   find_similar_extra: dict,
                   ) -> List[dict]:
        """
        Construct the query to execute against ApertureDB.
        This is used to build the query based on the columns and options.
        """
        query = [{
            self._options.command: {
                **self._options.extra,
                **({"results": {"list": list(columns)}} if columns else {}),
                "batch": {
                    "batch_id": 0,
                    "batch_size": batch_size
                },
                **({"blobs": True} if blobs else {}),
                **({"as_format": as_format} if as_format else {}),
                **({"operations": operations} if operations else {}),
                **(find_similar_extra),
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

        if "batch" not in response[0][self._options.command]:
            # Some commands (like FindConnection) don't handle batching, so we assume all results are returned at once.
            logger.info(
                f"Single batch found for query: {query} -> {response[:10]}")
            return None

        batch_id = response[0][self._options.command]["batch"]["batch_id"]
        total_elements = response[0][self._options.command]["batch"]["total_elements"]
        end = response[0][self._options.command]["batch"]["end"]

        if end >= total_elements:  # No more batches to process
            return None

        next_query = query.copy()
        next_query[0][self._options.command]["batch"]["batch_id"] += 1
        return next_query

    def _get_query_results(self,
                           query: List[dict],
                           query_blobs: List[bytes],
                           ) -> Generator[Tuple[dict, Optional[bytes]], None, List[dict]]:
        logger.debug(f"Executing query: {query}")

        start_time = datetime.now()
        _, results, response_blobs = get_pool().execute_query(query, query_blobs)
        elapsed_time = datetime.now() - start_time
        logger.info(
            f"Query executed in {elapsed_time.total_seconds()} seconds. Results: {results}, Blobs: {len(response_blobs) if response_blobs else 0}")

        if not results or len(results) != 1:
            logger.warning(
                f"No results found for entity query. {query} -> {results}")
            raise ValueError(
                f"No results found for entity query: {query}. Please check the class and columns. Results: {results}")

        result_objects = results[0].get(
            self._options.command, {}).get(self._options.result_field, [])
        if not response_blobs:
            for row in result_objects:
                yield row, None
        else:
            for row, blob in zip_longest(result_objects, response_blobs):
                yield row or {}, blob

        return results

    def _add_non_list_columns(self, row: dict, non_list_columns: Set[str], quals: List[dict]) -> dict:
        """
        Add non-list columns to the row based on the quals.
        This is used to ensure that all requested columns are present in the result.

        This is necessary because PostgreSQL will not return rows that don't meet the quals, and it doesn't know that we're
        using special columns to do magic.

        So we just copy the qual constraints into the row.
        """
        row = row.copy()  # Avoid modifying the original row
        for col in non_list_columns:
            assert col not in row, f"Column {col} should not be in the row. It is a non-list column."

            for qual in quals:
                if qual.field_name == col and qual.operator == "=":
                    row[col] = qual.value
                    break
            else:
                row[col] = None
                logger.warning(
                    f"Column {col} not found in quals. It is a non-list column, but no qual was found for it. This may lead to unexpected results.")
        return row

    def execute(self, quals: List[Qual], columns: Set[str]) -> Generator[dict, None, None]:
        """ Execute the FDW query with the given quals and columns.

        Args:
            quals (list): List of conditions to filter the results.
                Note that filtering is optional because PostgreSQL will also filter the results.
            columns (set): List of columns to return in the results.
        """

        logger.info(
            f"Executing FDW {self._options.type}/{self._options.class_} with quals: {quals} and columns: {columns}")

        blobs = self._options.blob_column is not None and self._options.blob_column in columns
        list_columns = {col for col in columns if self._columns[col].listable}
        non_list_columns = {
            col for col in columns if not self._columns[col].listable}

        batch_size = BATCH_SIZE_WITH_BLOBS if blobs else BATCH_SIZE
        as_format = self._get_as_format(quals)
        operations = self._get_operations(quals)
        find_similar, find_similar_extra, vector = \
            self._get_find_similar(quals)

        query_blobs = [vector] if find_similar else []

        query = self._get_query(
            columns=list_columns,
            blobs=blobs,
            as_format=as_format,
            operations=operations,
            batch_size=batch_size,
            find_similar_extra=find_similar_extra,)

        n_results = 0
        exhausted = False
        try:
            while query:
                gen = self._get_query_results(query, query_blobs)
                try:
                    while True:
                        row, blob = next(gen)

                        # Add blob to the row if it exists
                        if blobs:
                            row[self._options.blob_column] = blob

                        result = self._normalize_row(columns, row)

                        result = self._add_non_list_columns(
                            result, non_list_columns, quals)

                        logger.debug(
                            f"Yielding row: {json.dumps({k: v[:10] if isinstance(v, str) else len(v) if isinstance(v, (bytes, list)) else v for k, v in row.items()}, indent=2)}"
                        )
                        n_results += 1
                        if n_results % 1000 == 0:
                            logger.info(
                                f"Yielded {n_results} results so far for FDW {self._options.type}/{self._options.class_}")
                        yield result
                except StopIteration as e:
                    response = e.value  # return value from _get_query_results

                query = self._get_next_query(query, response)
            exhausted = True
        finally:
            logger.info(
                f"Executed FDW {self._options.type}/{self._options.class_} with {n_results} results, {'exhausted' if exhausted else 'not exhausted'}.")

    @classmethod
    def import_schema(cls, schema, srv_options, options, restriction_type, restricts):
        """
        Import the schema from ApertureDB and return a list of TableDefinitions.
        This method is called when the foreign data wrapper is created.
        The result of this is to create the foreign tables in PostgreSQL.

        Note that we cannot add comments, foreign keys, or other constraints here.

        This method is called once per schema.
        """
        try:
            # Put these here for better error handling
            from .system import system_schema
            from .entity import entity_schema
            from .connection import connection_schema
            from .descriptor import descriptor_schema

            logger.info(f"Importing schema {schema} with options: {options}")
            if schema == "system":
                return system_schema()
            elif schema == "entity":
                return entity_schema()
            elif schema == "connection":
                return connection_schema()
            elif schema == "descriptor":
                return descriptor_schema()
            else:
                raise ValueError(f"Unknown schema: {schema}")
        except:
            logger.exception(
                f"Error importing schema {schema}: {sys.exc_info()[1]}")
            flush_logs()
            raise
        logger.info(f"Schema {schema} imported successfully")


print("FDW class defined successfully")
