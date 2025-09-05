import pytest
from aperturedb.CommonLibrary import execute_query
import pandas as pd
from common import (
    db_connection,
    calculate_text_scores,
    create_text_comparison_dataframe,
    assert_score_threshold
)




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
                "is_connected_to": {"ref": 1},
                "group_by_source": True,
                "results": {
                    "list": ["_uniqueid"]
                },
            }
        },
        {
            "FindDescriptor": {
                "set": "wf_embeddings_clip",
                "is_connected_to": {"ref": 2},
                "group_by_source": True,
                "results": {
                    "list": ["_uniqueid"]
                },
            }
        }
    ]

    status, response, _ = execute_query(db_connection, query)

    assert status == 0, f"Query failed: {response}"
    return response


@pytest.fixture(scope="module")
def count_items(run_query):
    assert len(run_query[0]["FindVideo"]["entities"]) == 1, f"Expected 1 video, got {len(run_query[0]['FindVideo']['entities'])}"
    assert len(run_query[1]["FindClip"]["entities"]) == 245, f"Expected 245 clip, got {len(run_query[1]['FindClip']['entities'])}"
    assert len(run_query[2]["FindDescriptor"]["entities"]) == 245, f"Expected 245 descriptor, got {len(run_query[2]['FindDescriptor']['entities'])}"
