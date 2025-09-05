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
            "FindImage": {
                "results": {
                    "list": ["_uniqueid", "filename", "expected_text", "corpus", "wf_ocr_done"]
                },
                "_ref": 1,
            }
        },
        {
            "FindEntity": {
                "with_class": "ExtractedText",
                "is_connected_to": {"ref": 1},
                "group_by_source": True,
                "results": {
                    "list": ["_uniqueid", "text"],
                },
                "_ref": 2,
            }
        },
        {
            "FindDescriptor": {
                "set": "wf_ocr_images",
                "is_connected_to": {"ref": 2},
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


def test_all_images_done(run_query):
    response = run_query
    not_done = [e for e in response[0]['FindImage'].get(
        'entities', []) or [] if not e['wf_ocr_done']]
    assert not not_done, f"Not all images marked as done: {not_done}"


def test_all_images_have_text(run_query):
    """Test that all images have extracted text."""
    response = run_query
    images = {e['_uniqueid']: e['filename']
              for e in response[0]['FindImage'].get('entities', []) or []}

    text_groups = set(response[1]['FindEntity'].get('entities', {}) or {})

    if text_groups != set(images.keys()):
        missing = {images[mid]
                   for mid in set(images.keys()) - text_groups}
        extra = text_groups - set(images.keys())
        assert False, f"Image text groups do not match images. Found {len(images)} images and {len(text_groups)} text groups. Missing: {missing}, Extra: {extra}"


def test_all_texts_have_descriptors(run_query):
    """Test that all texts have descriptors."""
    response = run_query
    text_ids = set(e['_uniqueid']
                   for ee in (response[1]['FindEntity'].get('entities', {}) or {}).values()
                   for e in ee)
    print(f"text_ids: {sorted(text_ids)}")

    descriptor_ids = set(
        response[2]['FindDescriptor'].get('entities', {}) or {})
    print(f"descriptor_ids: {sorted(descriptor_ids)}")

    if text_ids != descriptor_ids:
        missing = text_ids - descriptor_ids
        assert False, f"Text ids do not match descriptors. Missing: {sorted(missing)}"


@pytest.fixture(scope="module")
def calculate_scores(run_query):
    """Test that the BLEU scores are above a certain threshold."""
    response = run_query
    images = (response[0]['FindImage'].get('entities', []) or [])

    # Convert image text entities to descriptor-like format for consistency
    descriptor_groups = {}
    for image_id, text_entities in (response[1]['FindEntity'].get('entities', {}) or {}).items():
        if text_entities:
            # Extract text from the first text entity
            text = text_entities[0].get('text', '')
            if text:
                descriptor_groups[image_id] = [{'text': text}]

    assert images, f"No images found: {response}"
    assert descriptor_groups, f"No image texts found: {response}"

    # Use the same generic function as PDF tests
    df = create_text_comparison_dataframe(images, descriptor_groups)
    return calculate_text_scores(df)


@pytest.mark.parametrize("metric, corpus, threshold", [
    ("char_bleu_score", "signs", 0.05),
    ("bleu_score", "documents", 0.4),
    ("levenshtein_distance", "documents", 60),
    ("jaccard_distance", "documents", 0.2),
])
def test_mean_score(calculate_scores, metric, corpus, threshold):
    """Test that the mean score is above/below a certain threshold."""
    df = calculate_scores
    assert_score_threshold(df, metric, corpus, threshold)
