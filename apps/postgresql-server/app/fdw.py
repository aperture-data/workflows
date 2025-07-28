from multicorn import ForeignDataWrapper


class FDW(ForeignDataWrapper):
    def execute(self, quals, columns):
        print("Executing FDW with quals:", quals, "and columns:", columns)
        return []

    def import_schema(self, schema, srv_options, options, restriction_type,
                      restricts):
        print(f"Importing schema: {schema} with server options: {srv_options}, options: {options}, "
              f"restriction_type: {restriction_type}, restricts: {restricts}")
        return [
            {
                "table_name": "demo_table",
                "columns": [("id", "integer"), ("name", "text")],
                "options": {"dummy_option": "value"}
            }
        ]
