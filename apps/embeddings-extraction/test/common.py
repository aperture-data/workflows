import os
import pytest
import numpy as np
from aperturedb.Connector import Connector
from aperturedb.CommonLibrary import execute_query
from nltk.translate.bleu_score import sentence_bleu, SmoothingFunction
from nltk.metrics.distance import edit_distance, jaccard_distance
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def compute_bleu(candidate: str, reference: str) -> float:
    """Compute BLEU score between candidate and reference text."""
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
    """Compute character-level BLEU score between candidate and reference text."""
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


def calculate_text_scores(df: pd.DataFrame, corpus_field="corpus") -> pd.DataFrame:
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
    means = df.melt(id_vars=[corpus_field],
                    value_vars=["bleu_score", "char_bleu_score", "levenshtein_distance", 
                "jaccard_distance"], var_name="metric", value_name="score").groupby([corpus_field, "metric"]).agg([
        "mean", "std", "min", "max"]).reset_index()
    logger.info("means:\n%s", means.to_string())
    
    return df


def create_text_comparison_dataframe(entities, descriptor_groups, corpus_field="corpus", filename_field="filename"):
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
        columns=[corpus_field, filename_field, 'reference', 'hypothesis'],
        data=[
            [e.get(corpus_field, 'unknown'), e[filename_field], e.get('expected_text', ''), entity_texts.get(e['_uniqueid'])]
            for e in entities
        ])

    return df


def assert_score_threshold(df: pd.DataFrame, metric: str, corpus: str, threshold: float):
    """Assert that the mean score meets the threshold."""
    df_filtered = df[df["corpus"] == corpus]
    assert not df_filtered.empty, f"No data for corpus {corpus}"
    mean = df_filtered[metric].mean()

    higher_is_better = "distance" not in metric.lower()

    if higher_is_better:
        assert mean > threshold, f"Mean {metric} for {corpus} is below {threshold}: {mean}"
    else:
        assert mean < threshold, f"Mean {metric} for {corpus} is above {threshold}: {mean}"
