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
            "FindBlob": {
                "constraints": {
                    "document_type": ["==", "pdf"],
                    "pdf_type": ["==", "image"]
                },
                "results": {
                    "list": ["filename", "_uniqueid", "expected_text", "corpus"]
                },
                "_ref": 1,
            }
        },
        {
            "FindDescriptor": {
                "set": "wf_embeddings_clip_pdf_extraction",
                "is_connected_to": {"ref": 1},
                "group_by_source": True,
                "results": {
                    "list": ["_uniqueid", "text", "type", "page_number"],
                },
                "_ref": 2,
            }
        }
    ]

    status, response, _ = execute_query(db_connection, query)

    assert status == 0, f"Query failed: {response}"

    return response


def test_image_pdfs_have_descriptors(run_query):
    """Test that image PDFs have descriptors."""
    response = run_query
    pdfs = {e['_uniqueid']: e['filename']
            for e in response[0]['FindBlob']['entities']}

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
        assert descriptor.get('type') == 'extracted_from_pdf_image', \
            f"Descriptor {descriptor['_uniqueid']} has incorrect type: {descriptor.get('type')}"


@pytest.fixture(scope="module")
def calculate_scores(run_query):
    """Test that the BLEU scores are above a certain threshold."""
    response = run_query
    pdfs = (response[0]['FindBlob'].get('entities', []) or [])
    
    # Group descriptors by PDF and concatenate text
    pdf_texts = {}
    for pdf_id, descriptors in (response[1]['FindDescriptor'].get('entities', {}) or {}).items():
        if descriptors:
            # Concatenate all text from descriptors for this PDF
            all_text = ' '.join([d.get('text', '') for d in descriptors])
            pdf_texts[pdf_id] = all_text

    assert pdfs, "No image PDFs found"
    assert pdf_texts, "No PDF texts found"

    df = pd.DataFrame(
        columns=['pdf_type', 'filename', 'reference', 'hypothesis'],
        data=[
            [e['pdf_type'], e['filename'], e['expected_text'], pdf_texts.get(e['_uniqueid'])]
            for e in pdfs
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
    means = df.melt(id_vars=["pdf_type"],
                    value_vars=["bleu_score", "char_bleu_score", "levenshtein_distance", 
                "jaccard_distance"], var_name="metric", value_name="score").groupby(["pdf_type", "metric"]).agg([
        "mean", "std", "min", "max"]).reset_index()
    logger.info("means:\n%s", means.to_string())
    return df


@pytest.mark.parametrize("metric, pdf_type, threshold", [
    ("char_bleu_score", "image", 0.05),
    ("bleu_score", "image", 0.3),
    ("levenshtein_distance", "image", 80),
    ("jaccard_distance", "image", 0.3),
])
def test_mean_score(calculate_scores, metric, pdf_type, threshold):
    """Test that the mean score is above/below a certain threshold."""
    df = calculate_scores[calculate_scores["pdf_type"] == pdf_type]
    assert not df.empty, f"No data for PDF type {pdf_type}"
    mean = df[metric].mean()

    if "distance" in metric: # lower is better for distance metrics
        assert mean < threshold, f"Mean {metric} for {pdf_type} is above {threshold}: {mean}"
    else: # higher is better for other metrics
        assert mean > threshold, f"Mean {metric} for {pdf_type} is below {threshold}: {mean}"


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
            {
                "FindDescriptor": {
                    "set": "wf_embeddings_clip_pdf_extraction",
                    "is_connected_to": {"ref": 1},
                    "results": {
                        "list": ["_uniqueid", "text", "type", "page_number"]
                    },
                }
            }
        }
    ]

    status, response, _ = execute_query(db_connection, query)
    assert status == 0, f"Query failed: {response}"

    pdfs = response[0]['FindBlob'].get('entities', [])
    assert not pdfs, "Text PDFs were found"

    descriptor_groups = set(
        response[1]['FindDescriptor'].get('entities', {}).keys())
    
    assert not descriptor_groups, "Descriptors were found for text PDFs"
