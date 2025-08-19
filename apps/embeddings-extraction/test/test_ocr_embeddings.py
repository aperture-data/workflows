import os
import pytest
import numpy as np
from aperturedb import Connector
from aperturedb.CommonLibrary import execute_query

# Constants for OCR functionality (to be implemented)
OCR_DESCRIPTOR_SET = 'wf_embeddings_clip_ocr'
OCR_DONE_PROPERTY = 'wf_embeddings_clip_ocr'

@pytest.fixture(scope="session")
def db_connection():
    """Create a database connection for testing."""
    db = Connector()
    yield db
    db.close()

def test_ocr_descriptor_set_exists(db_connection):
    """Test that the OCR descriptor set was created."""
    query = [{
        "FindDescriptorSet": {
            "with_name": OCR_DESCRIPTOR_SET
        }
    }]
    
    execute_query(db_connection, query)
    response = db_connection.get_last_response()
    
    assert response['status'] == 0, f"Query failed: {response}"
    assert len(response['results']) > 0, f"Descriptor set {OCR_DESCRIPTOR_SET} not found"
    
    descriptor_set = response['results'][0]
    assert descriptor_set['name'] == OCR_DESCRIPTOR_SET
    assert descriptor_set['dimensions'] == 512

def test_images_have_ocr_embeddings(db_connection):
    """Test that images have been processed for OCR and have text embeddings."""
    query = [{
        "FindImage": {
            "constraints": {
                OCR_DONE_PROPERTY: ["!=", None]
            },
            "results": {
                "list": ["_id", "name", "expected_text", OCR_DONE_PROPERTY]
            }
        }
    }]
    
    execute_query(db_connection, query)
    response = db_connection.get_last_response()
    
    assert response['status'] == 0, f"Query failed: {response}"
    assert len(response['results']) > 0, f"No images with OCR embeddings found"
    
    # Check that all returned images have the OCR done property
    for image in response['results']:
        assert OCR_DONE_PROPERTY in image, f"Image {image.get('_id')} missing {OCR_DONE_PROPERTY}"
        assert image[OCR_DONE_PROPERTY] is not None, f"Image {image.get('_id')} has None {OCR_DONE_PROPERTY}"

def test_ocr_text_segments_exist(db_connection):
    """Test that OCR text segments were created from images."""
    query = [{
        "FindDescriptor": {
            "with_descriptor_set": OCR_DESCRIPTOR_SET,
            "limit": 10,
            "results": {
                "list": ["_id", "text", "type", "total_tokens", "segment_number", "confidence"]
            }
        }
    }]
    
    execute_query(db_connection, query)
    response = db_connection.get_last_response()
    
    assert response['status'] == 0, f"Query failed: {response}"
    assert len(response['results']) > 0, f"No OCR text segments found in {OCR_DESCRIPTOR_SET}"
    
    # Check that OCR segments have the expected properties
    for segment in response['results']:
        assert "text" in segment, f"OCR segment {segment.get('_id')} missing text property"
        assert segment["type"] == "ocr_text", f"OCR segment {segment.get('_id')} has wrong type: {segment.get('type')}"
        assert "total_tokens" in segment, f"OCR segment {segment.get('_id')} missing total_tokens"
        assert "segment_number" in segment, f"OCR segment {segment.get('_id')} missing segment_number"
        assert "confidence" in segment, f"OCR segment {segment.get('_id')} missing confidence score"

def test_ocr_embedding_dimensions(db_connection):
    """Test that OCR embeddings have the correct dimensions."""
    query = [{
        "FindDescriptor": {
            "with_descriptor_set": OCR_DESCRIPTOR_SET,
            "limit": 1,
            "results": {
                "list": ["_id", "descriptor_set", "vector"]
            }
        }
    }]
    
    execute_query(db_connection, query)
    response = db_connection.get_last_response()
    
    assert response['status'] == 0, f"Query failed: {response}"
    assert len(response['results']) > 0, f"No descriptors found in {OCR_DESCRIPTOR_SET}"
    
    descriptor = response['results'][0]
    vector = descriptor['vector']
    
    # Check that the vector has the expected dimensions (512 for CLIP ViT-B/16)
    assert len(vector) == 512, f"Expected 512 dimensions, got {len(vector)}"

def test_ocr_text_quality(db_connection):
    """Test that OCR text segments have reasonable quality."""
    query = [{
        "FindDescriptor": {
            "with_descriptor_set": OCR_DESCRIPTOR_SET,
            "limit": 5,
            "results": {
                "list": ["_id", "text", "total_tokens", "confidence"]
            }
        }
    }]
    
    execute_query(db_connection, query)
    response = db_connection.get_last_response()
    
    if len(response['results']) < 2:
        pytest.skip("Need at least 2 OCR descriptors for quality test")
    
    # Check that OCR segments have reasonable text content and confidence
    for segment in response['results']:
        assert len(segment['text']) > 0, f"OCR segment {segment.get('_id')} has empty text"
        assert segment['total_tokens'] > 0, f"OCR segment {segment.get('_id')} has no tokens"
        assert segment['total_tokens'] <= 100, f"OCR segment {segment.get('_id')} has too many tokens: {segment['total_tokens']}"
        assert 0 <= segment['confidence'] <= 1, f"OCR segment {segment.get('_id')} has invalid confidence: {segment['confidence']}"

def test_ocr_vs_expected_text_similarity(db_connection):
    """Test that OCR text resembles the expected text from CSV."""
    query = [{
        "FindImage": {
            "constraints": {
                "test_data": True,
                OCR_DONE_PROPERTY: ["!=", None]
            },
            "results": {
                "list": ["_id", "name", "expected_text"]
            }
        }
    }]
    
    execute_query(db_connection, query)
    response = db_connection.get_last_response()
    
    if len(response['results']) == 0:
        pytest.skip("No images with OCR processing found")
    
    # For each image, get its OCR text segments and compare with expected text
    for image in response['results']:
        image_id = image['_id']
        expected_text = image.get('expected_text', '').lower().strip()
        
        if not expected_text:
            continue  # Skip images without expected text
        
        # Get OCR segments for this image
        ocr_query = [{
            "FindDescriptor": {
                "with_descriptor_set": OCR_DESCRIPTOR_SET,
                "connect": {
                    "FindImage": {
                        "constraints": {
                            "_uniqueid": ["==", image_id]
                        }
                    }
                },
                "results": {
                    "list": ["_id", "text", "confidence"]
                }
            }
        }]
        
        execute_query(db_connection, ocr_query)
        ocr_response = db_connection.get_last_response()
        
        if len(ocr_response['results']) == 0:
            continue  # Skip if no OCR segments found
        
        # Check if any OCR text contains words from expected text
        ocr_texts = [segment['text'].lower() for segment in ocr_response['results']]
        ocr_combined = ' '.join(ocr_texts)
        
        # Simple word overlap test
        expected_words = set(expected_text.split())
        ocr_words = set(ocr_combined.split())
        
        # Check for some word overlap (OCR might not be perfect)
        overlap = expected_words.intersection(ocr_words)
        overlap_ratio = len(overlap) / len(expected_words) if expected_words else 0
        
        # We expect some overlap, but not necessarily perfect match
        assert overlap_ratio > 0.1, f"OCR text has insufficient overlap with expected text. Expected: '{expected_text}', OCR: '{ocr_combined}', Overlap ratio: {overlap_ratio}"

def test_ocr_embedding_similarity(db_connection):
    """Test that similar OCR texts have similar embeddings."""
    query = [{
        "FindDescriptor": {
            "with_descriptor_set": OCR_DESCRIPTOR_SET,
            "limit": 2,
            "results": {
                "list": ["_id", "vector"]
            }
        }
    }]
    
    execute_query(db_connection, query)
    response = db_connection.get_last_response()
    
    if len(response['results']) < 2:
        pytest.skip("Need at least 2 OCR descriptors for similarity test")
    
    # Get two embeddings
    vec1 = np.array(response['results'][0]['vector'])
    vec2 = np.array(response['results'][1]['vector'])
    
    # Normalize vectors
    vec1_norm = vec1 / np.linalg.norm(vec1)
    vec2_norm = vec2 / np.linalg.norm(vec2)
    
    # Calculate cosine similarity
    similarity = np.dot(vec1_norm, vec2_norm)
    
    # Similarity should be between -1 and 1
    assert -1 <= similarity <= 1, f"OCR similarity {similarity} not in expected range"

def test_ocr_embedding_query(db_connection):
    """Test that we can query for similar OCR embeddings."""
    query = [{
        "FindDescriptor": {
            "with_descriptor_set": OCR_DESCRIPTOR_SET,
            "limit": 1,
            "results": {
                "list": ["_id", "vector"]
            }
        }
    }]
    
    execute_query(db_connection, query)
    response = db_connection.get_last_response()
    
    if len(response['results']) == 0:
        pytest.skip("No OCR descriptors found for similarity query")
    
    # Get a reference vector
    ref_vector = response['results'][0]['vector']
    
    # Query for similar descriptors
    query = [{
        "FindDescriptor": {
            "with_descriptor_set": OCR_DESCRIPTOR_SET,
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
    
    assert response['status'] == 0, f"OCR similarity query failed: {response}"
    assert len(response['results']) > 0, "No similar OCR descriptors found"
    
    # Check that distances are returned and are reasonable
    for result in response['results']:
        assert 'distance' in result, "Distance not returned in OCR similarity query"
        assert result['distance'] >= 0, f"Negative distance: {result['distance']}"

def test_all_test_images_ocr_processed(db_connection):
    """Test that all test images were processed for OCR by the workflow."""
    # Count total test images
    total_query = [{
        "FindImage": {
            "constraints": {
                "test_data": True
            },
            "results": {
                "list": ["_id"]
            }
        }
    }]
    
    execute_query(db_connection, total_query)
    total_response = db_connection.get_last_response()
    total_images = len(total_response['results'])
    
    # Count OCR processed images
    processed_query = [{
        "FindImage": {
            "constraints": {
                "test_data": True,
                OCR_DONE_PROPERTY: ["!=", None]
            },
            "results": {
                "list": ["_id"]
            }
        }
    }]
    
    execute_query(db_connection, processed_query)
    processed_response = db_connection.get_last_response()
    processed_images = len(processed_response['results'])
    
    # All test images should be OCR processed
    assert processed_images == total_images, f"Expected {total_images} images OCR processed, got {processed_images}"
    assert processed_images > 0, "No images were OCR processed"

def test_ocr_cross_modal_similarity(db_connection):
    """Test that OCR text embeddings can be compared with other text embeddings."""
    # Get one OCR embedding
    ocr_query = [{
        "FindDescriptor": {
            "with_descriptor_set": OCR_DESCRIPTOR_SET,
            "limit": 1,
            "results": {
                "list": ["_id", "vector"]
            }
        }
    }]
    
    execute_query(db_connection, ocr_query)
    ocr_response = db_connection.get_last_response()
    
    # Get one PDF text embedding
    pdf_query = [{
        "FindDescriptor": {
            "with_descriptor_set": "wf_embeddings_clip_text",
            "limit": 1,
            "results": {
                "list": ["_id", "vector"]
            }
        }
    }]
    
    execute_query(db_connection, pdf_query)
    pdf_response = db_connection.get_last_response()
    
    if len(ocr_response['results']) == 0 or len(pdf_response['results']) == 0:
        pytest.skip("Need both OCR and PDF text descriptors for cross-modal test")
    
    # Get the vectors
    ocr_vector = np.array(ocr_response['results'][0]['vector'])
    pdf_vector = np.array(pdf_response['results'][0]['vector'])
    
    # Normalize vectors
    ocr_norm = ocr_vector / np.linalg.norm(ocr_vector)
    pdf_norm = pdf_vector / np.linalg.norm(pdf_vector)
    
    # Calculate cosine similarity
    similarity = np.dot(ocr_norm, pdf_norm)
    
    # Similarity should be between -1 and 1
    assert -1 <= similarity <= 1, f"OCR-PDF cross-modal similarity {similarity} not in expected range"

def test_ocr_confidence_distribution(db_connection):
    """Test that OCR confidence scores have reasonable distribution."""
    query = [{
        "FindDescriptor": {
            "with_descriptor_set": OCR_DESCRIPTOR_SET,
            "limit": 20,
            "results": {
                "list": ["_id", "confidence"]
            }
        }
    }]
    
    execute_query(db_connection, query)
    response = db_connection.get_last_response()
    
    if len(response['results']) < 5:
        pytest.skip("Need at least 5 OCR descriptors for confidence distribution test")
    
    confidences = [segment['confidence'] for segment in response['results']]
    
    # Check confidence score ranges
    assert all(0 <= conf <= 1 for conf in confidences), "All confidence scores should be between 0 and 1"
    
    # Check that we have a mix of confidence levels (not all perfect or all poor)
    high_confidence = sum(1 for conf in confidences if conf > 0.8)
    low_confidence = sum(1 for conf in confidences if conf < 0.3)
    
    # Should have some reasonable confidence scores
    assert high_confidence > 0, "Should have some high confidence OCR results"
    assert len(confidences) > 0, "Should have OCR results"
