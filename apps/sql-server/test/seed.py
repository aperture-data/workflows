from aperturedb.CommonLibrary import execute_query
from aperturedb.Connector import Connector
from itertools import product
import os


def strip_null(d):
    """
    Strip null values from a dictionary.
    """
    return {k: v for k, v in d.items() if v is not None}


def load_testdata(client):
    """
    Create some test data for the constraint suite.
    """
    boolean_values = [None, False, True]
    number_values = [None, 0, 1, 2]
    string_values = [None, "", "a", "b", "c"]

    query = []
    for b, n, s in product(boolean_values, number_values, string_values):
        query.append({"AddEntity": {"class": "TestRow",
                                    "properties": strip_null({
                                        "b": b,
                                        "n": n,
                                        "s": s
                                    })}})

    ref = 1
    for i in range(5):
        query.append({"AddEntity": {"class": "SourceNode",
                                    "properties": {"source_key": i},
                                    "_ref": ref,
                                    }})
        query.append({"AddEntity": {"class": "DestinationNode",
                                    "properties": {"destination_key": i},
                                    "_ref": ref + 1,
                                    }})
        query.append({"AddConnection": {
            "src": ref,
            "dst": ref + 1,
            "class": "edge",
            "properties": {"edge_key": i},
        }})
        ref += 2

    execute_query(client, query)


if __name__ == "__main__":
    DB_HOST = os.getenv("DB_HOST", "aperturedb")
    DB_PORT = int(os.getenv("DB_PORT", "55555"))
    DB_USER = os.getenv("DB_USER", "admin")
    DB_PASS = os.getenv("DB_PASS", "admin")
    print(f"{DB_HOST=}, {DB_PORT=}, {DB_USER=}, {DB_PASS}")
    client = Connector(host=DB_HOST, user=DB_USER,
                       port=DB_PORT, password=DB_PASS)

    print(f"host={client.host}")
    load_testdata(client)
    print("Test data loaded successfully.")

    _, results, _ = execute_query(client, [{"GetSchema": {}}])

    print("Loaded test data:", results)
