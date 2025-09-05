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
                "constraints": {
                    "document_type": ["==", "pdf"],
                    "corpus": ["==", "images"]
                },
                "results": {
                    "list": ["filename", "_uniqueid", "expected_text", "corpus", "wf_ocr_done"]
                },
                "_ref": 1,
            }
        },
        {
            "FindDescriptor": {
                "set": "wf_ocr_pdfs",
                "is_connected_to": {"ref": 1},
                "group_by_source": True,
                "results": {
                    "list": ["_uniqueid", "text", "type", "page_number", "source_type", "ocr_method"],
                },
                "_ref": 2,
            }
        }
    ]

    status, response, _ = execute_query(db_connection, query)

    assert status == 0, f"Query failed: {response}"

    return response


def test_all_pdfs_done(run_query):
    response = run_query
    not_done = [e for e in response[0]['FindBlob'].get(
        'entities', []) or [] if not e['wf_ocr_done']]
    assert not not_done, f"Not all PDFs marked as done: {not_done}"


def test_image_pdfs_have_descriptors(run_query):
    """Test that image PDFs have descriptors."""
    response = run_query
    pdfs = {e['_uniqueid']: e['filename']
            for e in (response[0]['FindBlob'].get('entities', []) or [])}

    descriptor_groups = set(
        response[1]['FindDescriptor'].get('entities', {}).keys())

    if descriptor_groups != set(pdfs.keys()):
        missing = {pdfs[mid]
                   for mid in set(pdfs.keys()) - descriptor_groups}
        extra = descriptor_groups - set(pdfs.keys())
        assert False, f"Descriptor groups do not match image PDFs. Missing: {missing}, Extra: {extra}"


def test_descriptors_have_correct_type(run_query):
    """Test that descriptors have the correct type property."""
    response = run_query
    descriptors = []
    for group in response[1]['FindDescriptor'].get('entities', {}).values():
        descriptors.extend(group)

    for descriptor in descriptors:
        assert descriptor.get('type') == 'text', \
            f"Descriptor {descriptor['_uniqueid']} has incorrect type: {descriptor.get('type')}"
        assert descriptor.get('source_type') == 'pdf', \
            f"Descriptor {descriptor['_uniqueid']} has incorrect source_type: {descriptor.get('source_type')}"


@pytest.fixture(scope="module")
def calculate_scores(run_query):
    """Test that the BLEU scores are above a certain threshold."""
    response = run_query
    pdfs = (response[0]['FindBlob'].get('entities', []) or [])
    descriptor_groups = response[1]['FindDescriptor'].get('entities', {}) or {}

    assert pdfs, "No image PDFs found"
    assert descriptor_groups, "No PDF texts found"

    df = create_text_comparison_dataframe(pdfs, descriptor_groups)
    return calculate_text_scores(df)


@pytest.mark.parametrize("metric, corpus, threshold", [
    ("char_bleu_score", "images", 0.9),
    ("bleu_score", "images", 0.8),
    ("levenshtein_distance", "images", 40),
    ("jaccard_distance", "images", 0.1),
])
def test_mean_score(calculate_scores, metric, corpus, threshold):
    """Test that the mean score is above/below a certain threshold."""
    df = calculate_scores
    assert_score_threshold(df, metric, corpus, threshold)


def test_text_pdfs_are_skipped(db_connection):
    """Test that text PDFs are skipped (not processed by OCR)."""
    query = [
        {
            "FindBlob": {
                "constraints": {
                    "document_type": ["==", "pdf"],
                    "corpus": ["==", "text"]
                },
                "results": {
                    "list": ["filename", "_uniqueid"]
                },
                "_ref": 1,
            },
        },
        {
            "FindDescriptor": {
                "set": "wf_ocr_pdfs",
                "is_connected_to": {"ref": 1},
                "results": {
                    "list": ["_uniqueid", "text", "type", "page_number", "source_type", "ocr_method"]
                },
                "group_by_source": True,
            }
        }
    ]

    status, response, _ = execute_query(db_connection, query)
    assert status == 0, f"Query failed: {response}"

    descriptors = response[1]['FindDescriptor'].get('entities', {})
    assert not descriptors, "Descriptors were found for text PDFs"

    pdfs = response[0]['FindBlob'].get('entities', [])
    assert pdfs, "Text PDFs were not found"
