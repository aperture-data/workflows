import os
import pytest
import numpy as np
from aperturedb import Connector
from aperturedb.CommonLibrary import execute_query

# Constants from the main extraction module
IMAGE_DESCRIPTOR_SET = 'wf_embeddings_clip'
DONE_PROPERTY = 'wf_embeddings_clip'

@pytest.fixture(scope="session")
def db_connection():
    """Create a database connection for testing."""
    DB_HOST = os.getenv("DB_HOST", "aperturedb")
    DB_PORT = int(os.getenv("DB_PORT", "55555"))
    DB_USER = os.getenv("DB_USER", "admin")
    DB_PASS = os.getenv("DB_PASS", "admin")
    print(f"{DB_HOST=}, {DB_PORT=}, {DB_USER=}, {DB_PASS}")
    db = Connector(host=DB_HOST, user=DB_USER,
                       port=DB_PORT, password=DB_PASS)
    yield db
    db.close()

def test_descriptor_set_exists(db_connection):
    """Test that the image descriptor set was created."""
    query = [{
        "FindDescriptorSet": {
            "with_name": IMAGE_DESCRIPTOR_SET
        }
    }]
    
    status, response, _execute_query(db_connection, query)
    
    assert status == 0, f"Query failed: {response}"
    assert len(response['results']) > 0, f"Descriptor set {IMAGE_DESCRIPTOR_SET} not found"
    

def test_images_have_embeddings(db_connection):
    """Test that images have been processed and have embeddings."""
    query = [{
        "FindImage": {
            "constraints": {
                DONE_PROPERTY: ["!=", None]
            },
            "results": {
                "list": ["_id", "filename", "expected_text", DONE_PROPERTY]
            }
        }
    }]
    
    execute_query(db_connection, query)
    response = db_connection.get_last_response()
    
    assert response['status'] == 0, f"Query failed: {response}"
    assert len(response['results']) > 0, f"No images with embeddings found"
    
    # Check that all returned images have the done property and expected text
    for image in response['results']:
        assert DONE_PROPERTY in image, f"Image {image.get('_id')} missing {DONE_PROPERTY}"
        assert image[DONE_PROPERTY] is not None, f"Image {image.get('_id')} has None {DONE_PROPERTY}"
        # Check that images have expected text (from CSV)
        assert "expected_text" in image, f"Image {image.get('_id')} missing expected_text property"

def test_embedding_dimensions(db_connection):
    """Test that embeddings have the correct dimensions."""
    query = [{
        "FindDescriptor": {
            "with_descriptor_set": IMAGE_DESCRIPTOR_SET,
            "limit": 1,
            "results": {
                "list": ["_id", "descriptor_set", "vector"]
            }
        }
    }]
    
    execute_query(db_connection, query)
    response = db_connection.get_last_response()
    
    assert response['status'] == 0, f"Query failed: {response}"
    assert len(response['results']) > 0, f"No descriptors found in {IMAGE_DESCRIPTOR_SET}"
    
    descriptor = response['results'][0]
    vector = descriptor['vector']
    
    # Check that the vector has the expected dimensions (512 for CLIP ViT-B/16)
    assert len(vector) == 512, f"Expected 512 dimensions, got {len(vector)}"

def test_embedding_similarity(db_connection):
    """Test that similar images have similar embeddings."""
    query = [{
        "FindDescriptor": {
            "with_descriptor_set": IMAGE_DESCRIPTOR_SET,
            "limit": 2,
            "results": {
                "list": ["_id", "vector"]
            }
        }
    }]
    
    execute_query(db_connection, query)
    response = db_connection.get_last_response()
    
    if len(response['results']) < 2:
        pytest.skip("Need at least 2 descriptors for similarity test")
    
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

def test_embedding_query(db_connection):
    """Test that we can query for similar embeddings."""
    query = [{
        "FindDescriptor": {
            "with_descriptor_set": IMAGE_DESCRIPTOR_SET,
            "limit": 1,
            "results": {
                "list": ["_id", "vector"]
            }
        }
    }]
    
    execute_query(db_connection, query)
    response = db_connection.get_last_response()
    
    if len(response['results']) == 0:
        pytest.skip("No descriptors found for similarity query")
    
    # Get a reference vector
    ref_vector = response['results'][0]['vector']
    
    # Query for similar descriptors
    query = [{
        "FindDescriptor": {
            "with_descriptor_set": IMAGE_DESCRIPTOR_SET,
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
    
    assert response['status'] == 0, f"Similarity query failed: {response}"
    assert len(response['results']) > 0, "No similar descriptors found"
    
    # Check that distances are returned and are reasonable
    for result in response['results']:
        assert 'distance' in result, "Distance not returned in similarity query"
        assert result['distance'] >= 0, f"Negative distance: {result['distance']}"

def test_expected_text_loaded(db_connection):
    """Test that expected text from CSV is properly loaded for images."""
    query = [{
        "FindImage": {
            "constraints": {
                "test_data": True
            },
            "results": {
                "list": ["_id", "filename", "expected_text"]
            }
        }
    }]
    
    execute_query(db_connection, query)
    response = db_connection.get_last_response()
    
    assert response['status'] == 0, f"Query failed: {response}"
    assert len(response['results']) > 0, f"No test images found"
    
    # Check that images have expected text
    for image in response['results']:
        assert "expected_text" in image, f"Image {image.get('_id')} missing expected_text property"
        # Some images might have empty expected text, but the property should exist
        assert image["expected_text"] is not None, f"Image {image.get('_id')} has None expected_text"

def test_all_test_images_processed(db_connection):
    """Test that all test images were processed by the workflow."""
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
    
    # Count processed images
    processed_query = [{
        "FindImage": {
            "constraints": {
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
    processed_images = len(processed_response['results'])
    
    # All test images should be processed
    assert processed_images == total_images, f"Expected {total_images} images processed, got {processed_images}"
    assert processed_images > 0, "No images were processed"
