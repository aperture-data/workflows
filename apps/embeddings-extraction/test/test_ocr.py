import os
import pytest
import numpy as np
from aperturedb.Connector import Connector
from aperturedb.CommonLibrary import execute_query
from nltk.translate.bleu_score import sentence_bleu, SmoothingFunction
import itertools
import pandas as pd

def compute_bleu(candidate: str, reference: str) -> float:
    # TODO: Consider case-folding and punctuation removal

    # Tokenize (BLEU is based on tokens, not raw strings)
    candidate_tokens = candidate.split()
    reference_tokens = [reference.split()]  # list of reference token lists

    # Apply smoothing to avoid zero scores
    smoothie = SmoothingFunction().method4
    return sentence_bleu(reference_tokens, candidate_tokens, smoothing_function=smoothie)


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
                    "list": ["_uniqueid", "name", "expected_text"]
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


def test_all_images_have_text(run_query):
    """Test that all images have extracted text."""
    response = run_query
    images = {e['_unique_id']: e['name']
              for e in response[0]['FindImage'].get('entities', []) or []}
    text_groups = set(
        response[1]['FindEntity'].get('entities', {}) or {}).keys()

    if text_groups != set(images.keys()):
        missing = {images[mid]
                   for mid in set(images.keys()) - text_groups}
        extra = text_groups - set(images.keys())
        assert False, f"Image text groups do not match images. Missing: {missing}, Extra: {extra}"

def test_all_texts_have_descriptors(run_query):
    """Test that all texts have descriptors."""
    response = run_query
    images = {e['_unique_id']: e['name']
              for e in (response[0]['FindImage'].get('entities', []) or [])}
    
    text_ids = dict(itertools.chain.from_iterable(
        [(e['_uniqueid'], e['text']) 
            for e in (response[1]['FindEntity'].get('entities', {}) or {}).values()]))

    descriptor_ids = set(response[2]['FindDescriptor'].get('entities', {}) or {}).keys()

    if text_ids != descriptor_ids:
        missing = {text_ids[mid] for mid in descriptor_ids - set(text_ids.keys())}
        extra = {text_ids[mid] for mid in set(text_ids.keys()) - descriptor_ids}
        assert False, f"Text ids do not match descriptors. Missing: {missing}, Extra: {extra}"

@pytest.fixture(scope="module")
def calculate_bleu_scores(run_query):
    """Test that the BLEU scores are above a certain threshold."""
    response = run_query
    images = (response[0]['FindImage'].get('entities', []) or [])
    image_texts = {k: v.get(0, {}).get('text', '') 
                   for k, v in (response[1]['FindEntity'].get('entities', {}) or {}).items()}

    df = pd.DataFrame(
        columns = ['name', 'reference', 'hypothesis'],
        data = [
            [images[e['name']], e['expected_text'], image_texts.get(e['_uniqueid'])]
        ])
    df['score'] = df.apply(lambda row: compute_bleu(row['hypothesis'], row['reference']), axis=1)
    print(df)
    return df

def test_minimum_bleu_score(calculate_bleu_scores):
    """Test that the minimum BLEU score is above a certain threshold."""
    df = calculate_bleu_scores
    min_score = df['score'].min()
    assert min_score > 0.5, f"Minimum BLEU score is below 0.5: {min_score}"

def test_mean_bleu_score(calculate_bleu_scores):
    """Test that the mean BLEU score is above a certain threshold."""
    df = calculate_bleu_scores
    mean_score = df['score'].mean()
    assert mean_score > 0.5, f"Mean BLEU score is below 0.5: {mean_score}"