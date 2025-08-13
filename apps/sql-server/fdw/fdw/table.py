from .common import Curry
import logging
from pydantic import BaseModel
from typing import List, Dict, Any, Optional, Tuple


logger = logging.getLogger(__name__)


class TableOptions(BaseModel):
    """
    Options passed to the foreign table from `import_schema`.
    """
    # name of the table in PostgreSQL
    table_name: str
    # number of objects, probably from "matched" field in GetSchema response
    count: int = 0
    # command to execute, e.g. "FindEntity", "FindConnection", etc.
    command: str = "FindEntity"
    # field to look for in the response, e.g. "entities", "connections"
    result_field: str = "entities"
    # path keys for the table; see https://github.com/pgsql-io/multicorn2/blob/7ab7f0bcfe6052ebb318ed982df8dfd78ce5ee6a/python/multicorn/__init__.py#L215
    path_keys: List[Tuple[List[str], int]] = []

    # This hook is used to modify the command body before executing it.
    # It is passed the command body as `command_body`.
    # It should modify the command body in place.
    modify_command_body: Optional[Curry] = None

    def model_post_init(self, context: Any):
        """
        Validate the options after model initialization.
        """
        # Check that modify_command_body has a valid function signature
        if self.modify_command_body:
            self.modify_command_body.validate_signature({"command_body"})

    @classmethod
    def from_string(cls, options_str: Dict[str, str]) -> "TableOptions":
        """
        Create a TableOptions instance from a string dictionary.
        This is used to decode options from the foreign table definition.
        Postgres restricts options to be a string-valued dictionary.
        """
        return cls.model_validate_json(options_str["table_options"])

    def to_string(self) -> Dict[str, str]:
        """
        Convert TableOptions to a string dictionary.
        This is used to encode options for the foreign table definition.
        """
        return {"table_options": self.model_dump_json()}

    # Reject any extra fields that are not defined in the model.
    model_config = {
        "extra": "forbid"
    }


# Utility functions for Curry hooks


def literal(parameters: Dict[str, Any],
            command_body: Dict[str, Any]) -> None:
    """
    A TableOptions modify_command_body hook.

    Adds the value to the command body under the given name.
    This is used to modify the command body before executing it.
    """
    command_body.update(parameters)
