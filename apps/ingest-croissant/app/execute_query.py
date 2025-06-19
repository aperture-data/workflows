import json
import sys
from aperturedb.CommonLibrary import create_connector, execute_query

URL=sys.argv[1]

with open("delete_dataset_by_url.json") as in_file:
    query = json.load(in_file)
    query[0]["FindEntity"]["constraints"] = {
        "url" : ["==", URL]
    }
    client = create_connector()
    result, response, _ = execute_query(client, query)

    print("Result:", json.dumps(response, indent=2))