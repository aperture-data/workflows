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
    entity_types = [
        "MOVIE",
        "GENRE",
        "PROFESSIONAL",
        "KEYWORD",
        "PRODUCTION_COMPANY",
        "SPOKEN_LANGUAGE"
        ]
    query = [
        {
            "FindEntity": {
                "with_class": et,
                "results": {
                    "count": True
                }
            }
        } for et in entity_types]

    status, response, _ = execute_query(db_connection, query)
    assert status == 0, f"Query failed: {response}"
    return response

@pytest.fixture(scope="module")
def run_connection_query(db_connection):
    connection_triples = [
        ("MOVIE", "HAS_GENRE", "GENRE"),
        ("MOVIE", "HAS_CAST", "PROFESSIONAL"),
        ("MOVIE", "HAS_CREW", "PROFESSIONAL"),
        ("MOVIE", "HAS_KEYWORD", "KEYWORD"),
        ("MOVIE", "HAS_PRODUCTION_COMPANY", "PRODUCTION_COMPANY"),
        ("MOVIE", "HAS_SPOKEN_LANGUAGE", "SPOKEN_LANGUAGE")
    ]
    query = []
    i = 1
    for subject, predicate, target in connection_triples:
        query.extend([
            {
                "FindEntity": {
                    "_ref": i,
                    "with_class": subject,
                    "results": {
                        "count": True
                    }
                }
            },{
                "FindEntity": {
                    "with_class": target,
                    "is_connected_to": {
                        "ref": i,
                        "connection_class": predicate
                    },
                    "results": {
                        "count": True
                    }
                }
            }])
        i += 1
    status, response, _ = execute_query(db_connection, query)
    assert status == 0, f"Query failed: {response}"
    return response

def test_count_entities(run_query):
    response = run_query
    assert response[0]['FindEntity']['count'] == 4803
    assert response[1]['FindEntity']['count'] == 20
    assert response[2]['FindEntity']['count'] == 104842
    assert response[3]['FindEntity']['count'] == 9813
    assert response[4]['FindEntity']['count'] == 5047
    assert response[5]['FindEntity']['count'] == 87

def test_count_connections(run_connection_query):
    response = run_connection_query
    assert response[1]['FindEntity']['count'] == 20
    assert response[3]['FindEntity']['count'] == 54588
    assert response[5]['FindEntity']['count'] == 52885
    assert response[7]['FindEntity']['count'] == 9813
    assert response[9]['FindEntity']['count'] == 5047
    assert response[11]['FindEntity']['count'] == 87