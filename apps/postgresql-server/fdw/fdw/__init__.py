from multicorn import TableDefinition, ColumnDefinition, ForeignDataWrapper
import sys
from datetime import datetime


class FDW(ForeignDataWrapper):
    def execute(self, quals, columns):
        print("Executing FDW with quals:", quals, "and columns:", columns)
        return []

    @classmethod
    def import_schema(cls, schema, srv_options, options, restriction_type, restricts):
        return [
            TableDefinition(
                "my_table",
                columns=[
                    ColumnDefinition(column_name="id", type_name="integer"),
                    ColumnDefinition(column_name="name", type_name="text"),
                ],
                options={"some_option": "value"}
            )
        ]


print("FDW class defined successfully")
