import sys
from datetime import datetime
sys.path.insert(0, "/opt/venv/lib/python3.10/site-packages")

with open("/tmp/fdw_hook.log", "a") as f:
    # timestamp the log entry
    f.write(f"{datetime.now().isoformat()} - ")
    f.write("FDW hook loaded by: " + " ".join(sys.argv) + "\n")

    f.write("\n".join(sys.path))

with open("/tmp/fdw_hook.log", "a") as f:
    try:
        from multicorn import ForeignDataWrapper
        f.write("Successfully imported ForeignDataWrapper\n")
        f.flush()
    except Exception as e:
        import traceback
        f.write("FAILED to import multicorn:\n")
        traceback.print_exc(file=f)
        f.flush()
        raise


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


print("FDW class defined successfully")
