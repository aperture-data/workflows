import os
import pytest
import numpy as np
from aperturedb.Connector import Connector
from aperturedb.CommonLibrary import execute_query
from nltk.translate.bleu_score import sentence_bleu, SmoothingFunction
from nltk.metrics.distance import edit_distance, jaccard_distance
from typing import Optional
import pandas as pd
import logging
import json

logger = logging.getLogger(__name__)


def compute_bleu(candidate: str, reference: str) -> Optional[float]:
    """Compute BLEU score between candidate and reference text."""
    if candidate is None or reference is None:
        return None

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
    try:
        return sentence_bleu(reference_tokens, candidate_tokens, smoothing_function=smoothie)
    except ValueError:
        return None


def compute_character_level_bleu(candidate: str, reference: str) -> Optional[float]:
    """Compute character-level BLEU score between candidate and reference text."""
    if candidate is None or reference is None:
        return None

    # case-fold
    candidate = candidate.lower()
    reference = reference.lower()

    # Tokenize at character level
    candidate_tokens = [c for c in candidate if c.isalnum()]
    # list of reference token lists
    reference_tokens = [list(c for c in reference if c.isalnum())]

    # Apply smoothing to avoid zero scores
    smoothie = SmoothingFunction().method4
    try:
        return sentence_bleu(reference_tokens, candidate_tokens, smoothing_function=smoothie)
    except ValueError:
        return None


def compute_levenshtein_distance(s1: str, s2: str) -> Optional[int]:
    """Compute the Levenshtein distance between two strings."""
    if s1 is None or s2 is None:
        return None

    return edit_distance(s1, s2)


def compute_jaccard_distance(s1: str, s2: str) -> Optional[float]:
    """Compute the Jaccard distance between two strings."""
    if s1 is None or s2 is None:
        return None

    set1 = set(s1)
    set2 = set(s2)
    try:
        return jaccard_distance(set1, set2)
    except ValueError:
        return None


@pytest.fixture(scope="session")
def db_connection():
    """Create a database connection."""
    DB_HOST = os.getenv("DB_HOST", "aperturedb")
    DB_PORT = int(os.getenv("DB_PORT", "55555"))
    DB_USER = os.getenv("DB_USER", "admin")
    DB_PASS = os.getenv("DB_PASS", "admin")
    CA_CERT = os.getenv("CA_CERT", None)
    return Connector(host=DB_HOST, user=DB_USER, port=DB_PORT, password=DB_PASS, ca_cert=CA_CERT)

@pytest.fixture(scope="session", autouse=True)
def print_schema(db_connection):
    print("Printing schema of the database...")
    query = [{"GetSchema": {}}]
    status, response, _ = execute_query(db_connection, query)
    assert status == 0, f"Failed to get schema: {response}"
    print(json.dumps(response, indent=2))



def create_text_comparison_dataframe(entities, descriptor_groups):
    """Create a dataframe for text comparison from entities (PDFs, images, etc.) and their descriptors."""
    # Group descriptors by entity and concatenate text
    entity_texts = {}
    for entity_id, descriptors in (descriptor_groups or {}).items():
        if descriptors:
            # Concatenate all text from descriptors for this entity
            all_text = ' '.join([d.get('text', '') for d in descriptors])
            entity_texts[entity_id] = all_text

    assert entities, "No entities found"
    assert entity_texts, "No entity texts found"

    df = pd.DataFrame(
        columns=["corpus", "filename", "reference", "hypothesis"],
        data=[
            [e.get("corpus"), e["filename"], e.get("expected_text", ""), entity_texts.get(e["_uniqueid"])]
            for e in entities
        ])


    return df


def calculate_text_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate text quality scores for a dataframe with reference and hypothesis columns."""
    df['bleu_score'] = df.apply(lambda row: compute_bleu(
        row['hypothesis'], row['reference']), axis=1)
    df['char_bleu_score'] = df.apply(lambda row: compute_character_level_bleu(
        row['hypothesis'], row['reference']), axis=1)
    df['levenshtein_distance'] = df.apply(lambda row: compute_levenshtein_distance(
        row['hypothesis'], row['reference']), axis=1)
    df['jaccard_distance'] = df.apply(lambda row: compute_jaccard_distance(
        row['hypothesis'], row['reference']), axis=1)

    # Log calculated scores
    logger.info("calculated scores:\n%s", df.to_json(orient='records'))

    # Calculate summary statistics
    means = df.melt(id_vars=["corpus"],
                    value_vars=["bleu_score", "char_bleu_score", "levenshtein_distance",
                "jaccard_distance"], var_name="metric", value_name="score").groupby(["corpus", "metric"]).agg([
        "mean", "std", "min", "max"]).reset_index()
    logger.info("means:\n%s", means.to_string())

    return df





def assert_score_threshold(df: pd.DataFrame, metric: str, corpus: str, threshold: float):
    """Assert that the mean score meets the threshold."""
    assert metric is not None, f"Metric parameter is None"
    df_filtered = df[df["corpus"] == corpus]
    assert not df_filtered.empty, f"No data for corpus {corpus}"
    mean = df_filtered[metric].mean() # excluding nulls

    higher_is_better = "distance" not in metric

    if higher_is_better:
        assert mean > threshold, f"Mean {metric} for {corpus} is below {threshold}: {mean}"
    else:
        assert mean < threshold, f"Mean {metric} for {corpus} is above {threshold}: {mean}"
