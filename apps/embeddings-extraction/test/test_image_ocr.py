import os
import pytest
import numpy as np
from aperturedb.Connector import Connector
from aperturedb.CommonLibrary import execute_query
from nltk.translate.bleu_score import sentence_bleu, SmoothingFunction
from nltk.metrics.distance import edit_distance, jaccard_distance
import itertools
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def compute_bleu(candidate: str, reference: str) -> float:
    # case-fold and extract alphanumeric tokens
    candidate = candidate.lower()
    reference = reference.lower()
    reference = ''.join(c for c in reference if c.isalnum() or c.isspace())
    candidate = ''.join(c for c in candidate if c.isalnum() or c.isspace())

    # Tokenize (BLEU is based on tokens, not raw strings)
    candidate_tokens = candidate.split()
    reference_tokens = [reference.split()]  # list of reference token lists

    # Apply smoothing to avoid zero scores
    smoothie = SmoothingFunction().method4
    return sentence_bleu(reference_tokens, candidate_tokens, smoothing_function=smoothie)


def compute_character_level_bleu(candidate: str, reference: str) -> float:
    # case-fold
    candidate = candidate.lower()
    reference = reference.lower()

    # Tokenize at character level
    candidate_tokens = [c for c in candidate if c.isalnum()]
    # list of reference token lists
    reference_tokens = [list(c for c in reference if c.isalnum())]

    # Apply smoothing to avoid zero scores
    smoothie = SmoothingFunction().method4
    return sentence_bleu(reference_tokens, candidate_tokens, smoothing_function=smoothie)


def compute_levenshtein_distance(s1: str, s2: str) -> int:
    """Compute the Levenshtein distance between two strings."""
    return edit_distance(s1, s2)


def compute_jaccard_distance(s1: str, s2: str) -> float:
    """Compute the Jaccard distance between two strings."""
    set1 = set(s1)
    set2 = set(s2)
    return jaccard_distance(set1, set2)


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
                    "list": ["_uniqueid", "name", "expected_text", "corpus"]
                },
                "_ref": 1,
            }
        },
        {
            "FindEntity": {
                "with_class": "ImageExtractedText",
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
                "set": "wf_embeddings_clip_image_extraction",
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


def test_all_images_have_text(run_query):
    """Test that all images have extracted text."""
    response = run_query
    images = {e['_uniqueid']: e['name']
              for e in response[0]['FindImage'].get('entities', []) or []}

    text_groups = set(response[1]['FindEntity'].get('entities', {}) or {})

    if text_groups != set(images.keys()):
        missing = {images[mid]
                   for mid in set(images.keys()) - text_groups}
        extra = text_groups - set(images.keys())
        assert False, f"Image text groups do not match images. Missing: {missing}, Extra: {extra}"


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
    image_texts = {k: v[0].get('text', '') if v else None
                   for k, v in (response[1]['FindEntity'].get('entities', {}) or {}).items()}

    assert images, "No images found"
    assert image_texts, "No image texts found"

    df = pd.DataFrame(
        columns=['corpus', 'name', 'reference', 'hypothesis'],
        data=[
            [e['corpus'], e['name'], e['expected_text'], image_texts.get(e['_uniqueid'])]
            for e in images
        ])

    df['bleu_score'] = df.apply(lambda row: compute_bleu(
        row['hypothesis'], row['reference']), axis=1)
    df['char_bleu_score'] = df.apply(lambda row: compute_character_level_bleu(
        row['hypothesis'], row['reference']), axis=1)
    df['levenshtein_distance'] = df.apply(lambda row: compute_levenshtein_distance(
        row['hypothesis'], row['reference']), axis=1)
    df['jaccard_distance'] = df.apply(lambda row: compute_jaccard_distance(
        row['hypothesis'], row['reference']), axis=1)

    # print df as JSON
    logger.info("calculated scores:\n%s", df.to_json(orient='records'))
    means = df.melt(id_vars=["corpus"],
                    value_vars=["bleu_score", "char_bleu_score", "levenshtein_distance", 
                "jaccard_distance"], var_name="metric", value_name="score").groupby(["corpus", "metric"]).agg([
        "mean", "std", "min", "max"]).reset_index()
    logger.info("means:\n%s", means.to_string())
    return df

@pytest.mark.parametrize("metric, corpus, threshold", [
    ("char_bleu_score", "signs", 0.05),
    ("bleu_score", "documents", 0.4),
    ("levenshtein_distance", "documents", 60),
    ("jaccard_distance", "documents", 0.2),
])
def test_mean_score(calculate_scores, metric, corpus, threshold):
    """Test that the mean score is above/below a certain threshold."""
    df = calculate_scores[calculate_scores["corpus"] == corpus]
    assert not df.empty, f"No data for corpus {corpus}"
    mean = df[metric].mean()

    if "distance" in metric: # lower is better for distance metrics
        assert mean < threshold, f"Mean {metric} for {corpus} is above {threshold}: {mean}"
    else: # higher is better for other metrics
        assert mean > threshold, f"Mean {metric} for {corpus} is below {threshold}: {mean}"