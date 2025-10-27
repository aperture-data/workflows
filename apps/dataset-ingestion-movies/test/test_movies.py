import pytest
from aperturedb.CommonLibrary import execute_query
from aperturedb.Connector import Connector

@pytest.fixture(scope="session")
def db_connection():
    """Create a database connection."""
    return Connector(
        host="lenz",
        port=55551,
        user="admin",
        password="admin",
        ca_cert="/ca/ca.crt")

@pytest.fixture(scope="module")
def run_query(db_connection):
    """Get the results of the query."""
    query = [
        {
            "FindEntity": {
                "with_class": "MOVIE",
                "results": {
                    "count": True
                }
            }
        }
    ]
    status, response, _ = execute_query(db_connection, query)
    assert status == 0, f"Query failed: {response}"
    return response

def test_movies_ingested(run_query):
    response = run_query
    assert response[0]['FindEntity']['count'] > 4500