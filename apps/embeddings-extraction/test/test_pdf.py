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
            "FindBlob": {
                "constraints": {"document_type": ["==", "pdf"]},
                "results": {
                    "list": ["filename", "_uniqueid", "expected_text", "corpus"]
                },
                "_ref": 1,
            }
        },
        {
            "FindDescriptor": {
                "is_connected_to": {"ref": 1},
                "group_by_source": True,
                "results": {
                    "list": ["_uniqueid", "text", "type"]
                },
                "_ref": 2,
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


@pytest.fixture(scope="module")
def calculate_scores(run_query):
    """Test that the BLEU scores are above a certain threshold."""
    response = run_query
    pdfs = (response[0]['FindBlob'].get('entities', []) or [])
    descriptor_groups = response[1]['FindDescriptor'].get('entities', {}) or {}

    assert pdfs, "No PDFs found"
    assert descriptor_groups, "No PDF texts found"

    df = create_text_comparison_dataframe(pdfs, descriptor_groups, "corpus")
    return calculate_text_scores(df)


@pytest.mark.parametrize("metric, corpus, threshold", [
    ("char_bleu_score", "text", 0.05),
    ("bleu_score", "text", 0.4),
    ("levenshtein_distance", "text", 60),
    ("jaccard_distance", "text", 0.2),
])
def test_mean_score(calculate_scores, metric, corpus, threshold):
    """Test that the mean score is above/below a certain threshold."""
    df = calculate_scores
    assert_score_threshold(df, metric, corpus, threshold)
