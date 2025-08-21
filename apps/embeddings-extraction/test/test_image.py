import os
import pytest
import numpy as np
from aperturedb.Connector import Connector
from aperturedb.CommonLibrary import execute_query


@pytest.fixture(scope="session")
def db_connection():
    """Create a database connection."""
    DB_HOST = os.getenv("DB_HOST", "aperturedb")
    DB_PORT = int(os.getenv("DB_PORT", "55555"))
    DB_USER = os.getenv("DB_USER", "admin")
    DB_PASS = os.getenv("DB_PASS", "admin")
    return Connector(host=DB_HOST, user=DB_USER, port=DB_PORT, password=DB_PASS)

@pytest.fixture(scope="module")
def run_query(db_connection):
    """Get the results of the query."""
    query = [
        {
            "FindImage": {
                "results": {
                    "list": ["_uniqueid", "name"]
                },
                "_ref": 1,
            }
        },
        {
            "FindDescriptor": {
                "is_connected_to": {"ref": 1},
                "group_by_source": True,
                "results": {
                    "list": ["_uniqueid"]
                }
            }
        }
    ]

    status, response, _ = execute_query(db_connection, query)

    assert status == 0, f"Query failed: {response}"
    return response


def test_descriptors_for_each_image(run_query):
    """Test that there are descriptors for each image."""
    response = run_query
    images = {e['_uniqueid']: e['name']
              for e in response[0]['FindImage']['entities']}
    descriptor_groups = set(
        response[1]['FindDescriptor'].get('entities', {}).keys())

    if descriptor_groups != set(images.keys()):
        missing = {images[mid]
                   for mid in set(images.keys()) - descriptor_groups}
        extra = descriptor_groups - set(images.keys())
        assert False, f"Descriptor groups do not match images. Missing: {missing}, Extra: {extra}"


def test_descriptor_count_matches(run_query):
    """Test that there are descriptors for each image."""
    response = run_query
    images = {e['_uniqueid']: e['name']
              for e in response[0]['FindImage']['entities']}
    descriptor_groups = response[1]['FindDescriptor'].get('entities', {})
    non_unitary = [mid for mid in images.keys()
                   if mid in descriptor_groups and
                   len(descriptor_groups[mid]['descriptors']) != 1]

    assert not non_unitary, f"Expected 1 descriptor per image, got {[len(descriptor_groups[mid]['descriptors']) for mid in non_unitary]} for images {[images[mid] for mid in non_unitary]}"
