import pytest
import os
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

def get_entity_counts(db_connection, dataset_name):
    """Get the results of the query for a specific dataset."""
    entity_types = [
        "Movie",
        "Genre",
        "Professional",
        "Keyword",
        "ProductionCompany",
        "SpokenLanguage"
    ]
    query = [
        {
            "FindEntity": {
                "with_class": et,
                "constraints": {
                    "dataset_name": ["==", dataset_name]
                },
                "results": {
                    "count": True
                }
            }
        } for et in entity_types]

    status, response, _ = execute_query(db_connection, query)
    assert status == 0, f"Query failed: {response}"
    return response

def get_connection_counts(db_connection, dataset_name):
    connection_triples = [
        ("Movie", "HasGenre", "Genre"),
        ("Movie", "HasCast", "Professional"),
        ("Movie", "HasCrew", "Professional"),
        ("Movie", "HasKeyword", "Keyword"),
        ("Movie", "HasProductionCompany", "ProductionCompany"),
        ("Movie", "HasSpokenLanguage", "SpokenLanguage")
    ]
    query = []
    i = 1
    for subject, predicate, target in connection_triples:
        query.extend([
            {
                "FindEntity": {
                    "_ref": i,
                    "with_class": subject,
                    "constraints": {
                        "dataset_name": ["==", dataset_name]
                    },
                    "results": {
                        "count": True
                    }
                }
            },{
                "FindEntity": {
                    "with_class": target,
                    "constraints": {
                        "dataset_name": ["==", dataset_name]
                    },
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

def test_count_entities_full(db_connection):
    response = get_entity_counts(db_connection, "tmdb_5000")
    sample_count = int(os.environ.get("SAMPLE_COUNT", "-1"))
    if sample_count == 10:
        assert response[0]['FindEntity']['count'] == 10
        assert response[1]['FindEntity']['count'] == 9
        assert response[2]['FindEntity']['count'] == 1746
        assert response[3]['FindEntity']['count'] == 119
        assert response[4]['FindEntity']['count'] == 25
        assert response[5]['FindEntity']['count'] == 5
    else:
        assert response[0]['FindEntity']['count'] == 4803
        assert response[1]['FindEntity']['count'] == 20
        assert response[2]['FindEntity']['count'] == 104842
        assert response[3]['FindEntity']['count'] == 9813
        assert response[4]['FindEntity']['count'] == 5047
        assert response[5]['FindEntity']['count'] == 87

def test_count_connections_full(db_connection):
    response = get_connection_counts(db_connection, "tmdb_5000")
    sample_count = int(os.environ.get("SAMPLE_COUNT", "-1"))
    if sample_count == 10:
        assert response[1]['FindEntity']['count'] == 9
        assert response[3]['FindEntity']['count'] == 804
        assert response[5]['FindEntity']['count'] == 954
        assert response[7]['FindEntity']['count'] == 119
        assert response[9]['FindEntity']['count'] == 25
        assert response[11]['FindEntity']['count'] == 5
    else:
        assert response[1]['FindEntity']['count'] == 20
        assert response[3]['FindEntity']['count'] == 54588
        assert response[5]['FindEntity']['count'] == 52885
        assert response[7]['FindEntity']['count'] == 9813
        assert response[9]['FindEntity']['count'] == 5047
        assert response[11]['FindEntity']['count'] == 87

def test_count_entities_sample(db_connection):
    # This dataset is populated by dataset-ingestion-movies-sample
    response = get_entity_counts(db_connection, "tmdb_5000_sample")
    
    # Asserting sample dataset size (should always match the SAMPLE_COUNT=10 case)
    assert response[0]['FindEntity']['count'] == 10
    assert response[1]['FindEntity']['count'] == 9
    assert response[2]['FindEntity']['count'] == 1746
    assert response[3]['FindEntity']['count'] == 119
    assert response[4]['FindEntity']['count'] == 25
    assert response[5]['FindEntity']['count'] == 5

def test_count_connections_sample(db_connection):
    response = get_connection_counts(db_connection, "tmdb_5000_sample")
    
    assert response[1]['FindEntity']['count'] == 9
    assert response[3]['FindEntity']['count'] == 804
    assert response[5]['FindEntity']['count'] == 954
    assert response[7]['FindEntity']['count'] == 119
    assert response[9]['FindEntity']['count'] == 25
    assert response[11]['FindEntity']['count'] == 5
