import pytest
from aperturedb.CommonLibrary import execute_query
from common import db_connection


@pytest.fixture(scope="module")
def run_query(db_connection):
    """Get the results of the query."""
    query = [
        {
            "FindVideo": {
                "results": {
                    "list": ["_uniqueid", "filename"]
                },
                "_ref": 1,
            }
        },
        {
            "FindClip": {
                "_ref": 2,
                "is_connected_to": {"ref": 1},
                "results": {
                    "count": True
                },
            }
        },
        {
            "FindDescriptor": {
                "set": "wf_embeddings_clip_video",
                "is_connected_to": {"ref": 2},
                "results": {
                    "count": True
                },
            }
        }
    ]

    status, response, _ = execute_query(db_connection, query)

    assert status == 0, f"Query failed: {response}"
    return response

def test_count_items(run_query):
    # These numbers are based on the video chosen for the test.
    expected_clips = 245 + 5679
    assert len(run_query[0]["FindVideo"]["entities"]) == 2, f"Expected 1 video, got {len(run_query[0]['FindVideo']['entities'])}"
    assert run_query[1]["FindClip"]["count"] == expected_clips, \
        f"Expected {expected_clips} clips, got {run_query[1]['FindClip']['count']}"
    assert run_query[2]["FindDescriptor"]["count"] == expected_clips, \
        f"Expected {expected_clips} descriptors, got {run_query[2]['FindDescriptor']['count']}"
