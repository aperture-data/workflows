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
            "FindBlob": {
                "constraints": {"document_type": ["==", "pdf"]},
                "results": {
                    "list": ["filename", "_uniqueid"]
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


def test_descriptors_for_each_pdf(run_query):
    """Test that there are descriptors for each PDF."""
    response = run_query

    pdfs = {e['_uniqueid']: e['filename']
            for e in response[0]['FindBlob']['entities']}
    descriptor_groups = set(
        response[1]['FindDescriptor'].get('entities', {}).keys())

    if descriptor_groups != set(pdfs.keys()):
        missing = {pdfs[mid]
                   for mid in set(pdfs.keys()) - descriptor_groups}
        extra = descriptor_groups - set(pdfs.keys())
        assert False, f"Descriptor groups do not match PDFs. Missing: {missing}, Extra: {extra}"
