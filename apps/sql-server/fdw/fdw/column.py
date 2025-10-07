from .common import Curry, TYPE_MAP, PathKey
from .annotation import TableAnnotations
import logging
from pydantic import BaseModel
from typing import List, Dict, Any, Optional, Literal, Tuple, Iterator
from multicorn import ColumnDefinition
import json

logger = logging.getLogger(__name__)


class ColumnOptions(BaseModel):
    """
    Options passed to the foreign table columns from `import_schema`.
    """
    count: Optional[int] = None  # number of matched objects for this column
    indexed: bool = False  # whether the column is indexed
    # AQL type of the column: "string", "number", "boolean", "json", "blob"
    type: Optional[Literal["string",
                           "number", "boolean", "json", "blob", "datetime", "uniqueid"]] = None
    listable: bool = True  # whether the column can be passed to results/list
    unique: bool = False  # whether the column is unique, used for _uniqueid

    # These three hooks are used to provide special handling for certain columns.
    # All of them are invoked when the column is used in a qual with an equality operator.
    # All are passed the qual value as `value` and may take other keyword arguments.
    # Always use the `Curry` class to pass these functions as options.
    # This ensues they are serialized correctly and can be executed later.

    # modify_command_body: also passed `command_body` and expected to modify in place
    modify_command_body: Optional[Curry] = None

    # query_blobs: returns a list of bytes
    query_blobs: Optional[Curry] = None

    # post_process_results: also passed `row` (before type normalization) and expected to modify in place
    post_process_results: Optional[Curry] = None

    def model_post_init(self, context: Any):
        """
        Validate the options after model initialization.
        """
        if self.listable and not self.type:
            raise ValueError("listable columns must have a type defined")

        # Check that hooks have valid function signatures
        if self.modify_command_body:
            self.modify_command_body.validate_signature(
                {"value", "command_body"})
        if self.query_blobs:
            self.query_blobs.validate_signature({"value"})
        if self.post_process_results:
            self.post_process_results.validate_signature(
                {"value", "row", "blob"})

    @classmethod
    def from_string(cls, options_str: Dict[str, str]) -> "ColumnOptions":
        """
        Create a ColumnOptions instance from a string dictionary.
        This is used to decode options from the foreign table column definition.
        """
        try:
            options = json.loads(options_str["column_options"])
        except Exception as e:
            logger.error(f"Error parsing JSON: {e} {options_str=}")
            raise
        return cls(**options)

    def to_string(self) -> Dict[str, str]:
        """
        Convert ColumnOptions to a string dictionary.
        This is used to encode options for the foreign table column definition.
        """
        return {"column_options": json.dumps(self.dict(), default=str)}

    def path_keys(self, name: str) -> Iterator[PathKey]:
        """
        Return the path keys for this column.
        This is used to extract the path keys from the column definition.
        """
        if self.indexed or self.type == "uniqueid":
            expected_rows = 1 if name == "_uniqueid" else 1000  # See FDW.get_path_keys
            yield PathKey(columns=[name], expected_rows=expected_rows)
            if name == "_src":
                yield PathKey(columns=["_src", "_dst"], expected_rows=1)
            elif name == "_dst":
                yield PathKey(columns=["_dst", "_src"], expected_rows=1)

    # Reject any extra fields that are not defined in the model.
    model_config = {
        "extra": "forbid"
    }


# Some utility functions for Curry hooks


def passthrough(name: str,
                value: Any, command_body: Dict[str, Any]) -> None:
    """
    A ColumnOptions modify_command_body hook.

    Adds the value to the command body under the given name.
    """
    command_body[name] = value


def passthrough_lowercase(name: str,
                value: Any, command_body: Dict[str, Any]) -> None:
    """
    A ColumnOptions modify_command_body hook.

    Adds the value to the command body under the given name in lowercase.
    """
    command_body[name] = value.lower()


def add_blob(column: str,
             value: Any, row: dict, blob: Optional[bytes]) -> None:
    """
    A ColumnOptions post_process_results hook.

    Adds the value to the row under the given column name,
    if the value is true.
    """
    assert isinstance(value, bool), \
        f"Expected value to be a boolean, got {type(value)}"
    if value:
        row[column] = blob

# Some utility functions for creating column defintions


def blob_columns(column: str) -> List[ColumnDefinition]:
    """
    Constructs column definitions for object types that can contain blobs.

    This approach allows us to control whether the blobs are returned
    based on the query, while still providing a column for the blob data.
    In particular, the boolean column avoids the awkwardness of including the blob data in "SELECT *".

    Args:
        column (str): The name of the column to use for blobs, e.g. _blob

    Returns:
        column_definitions: A list of two column definitions:
            - A boolean column `_blobs` indicating if the query should return blobs
            - A blob column for the actual blob data
    """
    return [
        ColumnDefinition(
            column_name="_blobs",
            type_name="boolean",
            options=ColumnOptions(
                type="boolean",
                listable=False,
                modify_command_body=Curry(passthrough, "blobs"),
                post_process_results=Curry(add_blob, column=column)
            ).to_string()),
        ColumnDefinition(
            column_name=column,
            type_name="bytea",
            options=ColumnOptions(
                type="blob",
                listable=False,
            ).to_string())
    ]


def property_columns(data: dict, annotations: Optional[TableAnnotations] = None) -> List[ColumnDefinition]:
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
                    options=ColumnOptions(
                        count=count,
                        indexed=indexed,
                        type=type_.lower(),
                    ).to_string())
                )
            except Exception as e:
                logger.exception(
                    f"Error processing property '{prop}': {e}")
                raise

    columns.append(uniqueid_column(data.get("matched", 0)))
    if annotations:
        annotations.primary_key()

    return columns


def uniqueid_column(count: int = 0) -> ColumnDefinition:
    """ Create a ColumnDefinition for the _uniqueid column. """
    return ColumnDefinition(
        column_name="_uniqueid",
        type_name="text",
        options=ColumnOptions(
            count=count,
            indexed=True,
            unique=True,
            type="uniqueid"
        ).to_string())


def get_path_keys(columns: List[ColumnDefinition]) -> List[PathKey]:
    """
    Extract the path keys from the column definitions.
    """
    path_keys: List[PathKey] = []
    for col in columns:
        options = ColumnOptions.from_string(col.options)
        path_keys.extend(options.path_keys(col.column_name))
    return path_keys
