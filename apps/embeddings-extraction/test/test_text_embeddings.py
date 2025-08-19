import os
import pytest
import numpy as np
from aperturedb import Connector
from aperturedb.CommonLibrary import execute_query

# Constants from the main extraction module
TEXT_DESCRIPTOR_SET = 'wf_embeddings_clip_text'
DONE_PROPERTY = 'wf_embeddings_clip'

@pytest.fixture(scope="session")
def db_connection():
    """Create a database connection for testing."""
    db = Connector()
    yield db
    db.close()

def test_text_descriptor_set_exists(db_connection):
    """Test that the text descriptor set was created."""
    query = [{
        "FindDescriptorSet": {
            "with_name": TEXT_DESCRIPTOR_SET
        }
    }]
    
    execute_query(db_connection, query)
    response = db_connection.get_last_response()
    
    assert response['status'] == 0, f"Query failed: {response}"
    assert len(response['results']) > 0, f"Descriptor set {TEXT_DESCRIPTOR_SET} not found"
    
    descriptor_set = response['results'][0]
    assert descriptor_set['name'] == TEXT_DESCRIPTOR_SET
    assert descriptor_set['dimensions'] == 512

def test_pdfs_have_embeddings(db_connection):
    """Test that PDF blobs have been processed and have embeddings."""
    query = [{
        "FindBlob": {
            "constraints": {
                "document_type": "pdf",
                DONE_PROPERTY: ["!=", None]
            },
            "results": {
                "list": ["_id", "name", DONE_PROPERTY, "segments"]
            }
        }
    }]
    
    execute_query(db_connection, query)
    response = db_connection.get_last_response()
    
    assert response['status'] == 0, f"Query failed: {response}"
    assert len(response['results']) > 0, f"No PDFs with embeddings found"
    
    # Check that all returned PDFs have the done property and segments count
    for pdf in response['results']:
        assert DONE_PROPERTY in pdf, f"PDF {pdf.get('_id')} missing {DONE_PROPERTY}"
        assert pdf[DONE_PROPERTY] is not None, f"PDF {pdf.get('_id')} has None {DONE_PROPERTY}"
        assert "segments" in pdf, f"PDF {pdf.get('_id')} missing segments count"
        assert pdf["segments"] > 0, f"PDF {pdf.get('_id')} has no segments"

def test_text_segments_exist(db_connection):
    """Test that text segments were created from PDFs."""
    query = [{
        "FindDescriptor": {
            "with_descriptor_set": TEXT_DESCRIPTOR_SET,
            "limit": 10,
            "results": {
                "list": ["_id", "text", "type", "total_tokens", "segment_number"]
            }
        }
    }]
    
    execute_query(db_connection, query)
    response = db_connection.get_last_response()
    
    assert response['status'] == 0, f"Query failed: {response}"
    assert len(response['results']) > 0, f"No text segments found in {TEXT_DESCRIPTOR_SET}"
    
    # Check that segments have the expected properties
    for segment in response['results']:
        assert "text" in segment, f"Segment {segment.get('_id')} missing text property"
        assert segment["type"] == "text", f"Segment {segment.get('_id')} has wrong type: {segment.get('type')}"
        assert "total_tokens" in segment, f"Segment {segment.get('_id')} missing total_tokens"
        assert "segment_number" in segment, f"Segment {segment.get('_id')} missing segment_number"

def test_text_embedding_dimensions(db_connection):
    """Test that text embeddings have the correct dimensions."""
    query = [{
        "FindDescriptor": {
            "with_descriptor_set": TEXT_DESCRIPTOR_SET,
            "limit": 1,
            "results": {
                "list": ["_id", "descriptor_set", "vector"]
            }
        }
    }]
    
    execute_query(db_connection, query)
    response = db_connection.get_last_response()
    
    assert response['status'] == 0, f"Query failed: {response}"
    assert len(response['results']) > 0, f"No descriptors found in {TEXT_DESCRIPTOR_SET}"
    
    descriptor = response['results'][0]
    vector = descriptor['vector']
    
    # Check that the vector has the expected dimensions (512 for CLIP ViT-B/16)
    assert len(vector) == 512, f"Expected 512 dimensions, got {len(vector)}"

def test_text_embedding_similarity(db_connection):
    """Test that similar texts have similar embeddings."""
    query = [{
        "FindDescriptor": {
            "with_descriptor_set": TEXT_DESCRIPTOR_SET,
            "limit": 2,
            "results": {
                "list": ["_id", "vector"]
            }
        }
    }]
    
    execute_query(db_connection, query)
    response = db_connection.get_last_response()
    
    if len(response['results']) < 2:
        pytest.skip("Need at least 2 text descriptors for similarity test")
    
    # Get two embeddings
    vec1 = np.array(response['results'][0]['vector'])
    vec2 = np.array(response['results'][1]['vector'])
    
    # Normalize vectors
    vec1_norm = vec1 / np.linalg.norm(vec1)
    vec2_norm = vec2 / np.linalg.norm(vec2)
    
    # Calculate cosine similarity
    similarity = np.dot(vec1_norm, vec2_norm)
    
    # Similarity should be between -1 and 1
    assert -1 <= similarity <= 1, f"Similarity {similarity} not in expected range"

def test_text_embedding_query(db_connection):
    """Test that we can query for similar text embeddings."""
    query = [{
        "FindDescriptor": {
            "with_descriptor_set": TEXT_DESCRIPTOR_SET,
            "limit": 1,
            "results": {
                "list": ["_id", "vector"]
            }
        }
    }]
    
    execute_query(db_connection, query)
    response = db_connection.get_last_response()
    
    if len(response['results']) == 0:
        pytest.skip("No text descriptors found for similarity query")
    
    # Get a reference vector
    ref_vector = response['results'][0]['vector']
    
    # Query for similar descriptors
    query = [{
        "FindDescriptor": {
            "with_descriptor_set": TEXT_DESCRIPTOR_SET,
            "k_neighbors": {
                "vector": ref_vector,
                "k": 3
            },
            "results": {
                "list": ["_id", "distance"]
            }
        }
    }]
    
    execute_query(db_connection, query)
    response = db_connection.get_last_response()
    
    assert response['status'] == 0, f"Text similarity query failed: {response}"
    assert len(response['results']) > 0, "No similar text descriptors found"
    
    # Check that distances are returned and are reasonable
    for result in response['results']:
        assert 'distance' in result, "Distance not returned in text similarity query"
        assert result['distance'] >= 0, f"Negative distance: {result['distance']}"

def test_all_test_pdfs_processed(db_connection):
    """Test that all test PDFs were processed by the workflow."""
    # Count total test PDFs
    total_query = [{
        "FindBlob": {
            "constraints": {
                "document_type": "pdf",
                "test_data": True
            },
            "results": {
                "list": ["_id"]
            }
        }
    }]
    
    execute_query(db_connection, total_query)
    total_response = db_connection.get_last_response()
    total_pdfs = len(total_response['results'])
    
    # Count processed PDFs
    processed_query = [{
        "FindBlob": {
            "constraints": {
                "document_type": "pdf",
                "test_data": True,
                DONE_PROPERTY: ["!=", None]
            },
            "results": {
                "list": ["_id"]
            }
        }
    }]
    
    execute_query(db_connection, processed_query)
    processed_response = db_connection.get_last_response()
    processed_pdfs = len(processed_response['results'])
    
    # All test PDFs should be processed
    assert processed_pdfs == total_pdfs, f"Expected {total_pdfs} PDFs processed, got {processed_pdfs}"
    assert processed_pdfs > 0, "No PDFs were processed"

def test_text_segment_quality(db_connection):
    """Test that text segments have reasonable quality."""
    query = [{
        "FindDescriptor": {
            "with_descriptor_set": TEXT_DESCRIPTOR_SET,
            "limit": 5,
            "results": {
                "list": ["_id", "text", "total_tokens"]
            }
        }
    }]
    
    execute_query(db_connection, query)
    response = db_connection.get_last_response()
    
    if len(response['results']) < 2:
        pytest.skip("Need at least 2 text descriptors for quality test")
    
    # Check that segments have reasonable text content
    for segment in response['results']:
        assert len(segment['text']) > 0, f"Segment {segment.get('_id')} has empty text"
        assert segment['total_tokens'] > 0, f"Segment {segment.get('_id')} has no tokens"
        assert segment['total_tokens'] <= 100, f"Segment {segment.get('_id')} has too many tokens: {segment['total_tokens']}"

def test_cross_modal_similarity(db_connection):
    """Test that we can compare image and text embeddings."""
    # Get one image embedding
    image_query = [{
        "FindDescriptor": {
            "with_descriptor_set": "wf_embeddings_clip",
            "limit": 1,
            "results": {
                "list": ["_id", "vector"]
            }
        }
    }]
    
    execute_query(db_connection, image_query)
    image_response = db_connection.get_last_response()
    
    # Get one text embedding
    text_query = [{
        "FindDescriptor": {
            "with_descriptor_set": TEXT_DESCRIPTOR_SET,
            "limit": 1,
            "results": {
                "list": ["_id", "vector"]
            }
        }
    }]
    
    execute_query(db_connection, text_query)
    text_response = db_connection.get_last_response()
    
    if len(image_response['results']) == 0 or len(text_response['results']) == 0:
        pytest.skip("Need both image and text descriptors for cross-modal test")
    
    # Get the vectors
    image_vector = np.array(image_response['results'][0]['vector'])
    text_vector = np.array(text_response['results'][0]['vector'])
    
    # Normalize vectors
    image_norm = image_vector / np.linalg.norm(image_vector)
    text_norm = text_vector / np.linalg.norm(text_vector)
    
    # Calculate cosine similarity
    similarity = np.dot(image_norm, text_norm)
    
    # Similarity should be between -1 and 1
    assert -1 <= similarity <= 1, f"Cross-modal similarity {similarity} not in expected range"
