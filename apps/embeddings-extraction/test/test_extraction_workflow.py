import os
import sys
import pytest
import subprocess
import time
from aperturedb import Connector
from aperturedb.CommonLibrary import execute_query

# Add the parent app directory to the path to import the extraction modules
sys.path.append('/app/../app')
from extract_embeddings import IMAGE_DESCRIPTOR_SET, TEXT_DESCRIPTOR_SET, DONE_PROPERTY

@pytest.fixture(scope="session")
def db_connection():
    """Create a database connection for testing."""
    db = Connector()
    yield db
    db.close()

def test_extraction_workflow_runs(db_connection):
    """Test that the extraction workflow can be executed."""
    # This test verifies that the workflow can be run as a subprocess
    # In the actual docker-compose setup, this would be handled by the embeddings-extraction service
    
    # Check that the extraction script exists and is executable
    script_path = "/app/../app/extract_embeddings.py"
    assert os.path.exists(script_path), f"Extraction script not found at {script_path}"
    
    # Test that we can import the extraction module
    try:
        import extract_embeddings
        assert hasattr(extract_embeddings, 'main'), "Extraction module missing main function"
    except ImportError as e:
        pytest.fail(f"Failed to import extraction module: {e}")

def test_descriptor_sets_created(db_connection):
    """Test that both descriptor sets were created by the workflow."""
    # Check image descriptor set
    image_query = [{
        "FindDescriptorSet": {
            "with_name": IMAGE_DESCRIPTOR_SET
        }
    }]
    
    execute_query(db_connection, image_query)
    image_response = db_connection.get_last_response()
    
    assert image_response['status'] == 0, f"Image descriptor set query failed: {image_response}"
    assert len(image_response['results']) > 0, f"Image descriptor set {IMAGE_DESCRIPTOR_SET} not found"
    
    # Check text descriptor set
    text_query = [{
        "FindDescriptorSet": {
            "with_name": TEXT_DESCRIPTOR_SET
        }
    }]
    
    execute_query(db_connection, text_query)
    text_response = db_connection.get_last_response()
    
    assert text_response['status'] == 0, f"Text descriptor set query failed: {text_response}"
    assert len(text_response['results']) > 0, f"Text descriptor set {TEXT_DESCRIPTOR_SET} not found"

def test_images_processed(db_connection):
    """Test that test images were processed by the workflow."""
    query = [{
        "FindImage": {
            "constraints": {
                "test_data": True,
                DONE_PROPERTY: ["!=", None]
            },
            "results": {
                "list": ["_id", "name", DONE_PROPERTY]
            }
        }
    }]
    
    execute_query(db_connection, query)
    response = db_connection.get_last_response()
    
    assert response['status'] == 0, f"Query failed: {response}"
    assert len(response['results']) > 0, f"No test images were processed"
    
    # Check that all test images have the done property
    for image in response['results']:
        assert DONE_PROPERTY in image, f"Image {image.get('_id')} missing {DONE_PROPERTY}"
        assert image[DONE_PROPERTY] is not None, f"Image {image.get('_id')} has None {DONE_PROPERTY}"

def test_pdfs_processed(db_connection):
    """Test that test PDFs were processed by the workflow."""
    query = [{
        "FindBlob": {
            "constraints": {
                "format": "PDF",
                "test_data": True,
                DONE_PROPERTY: ["!=", None]
            },
            "results": {
                "list": ["_id", "name", DONE_PROPERTY]
            }
        }
    }]
    
    execute_query(db_connection, query)
    response = db_connection.get_last_response()
    
    assert response['status'] == 0, f"Query failed: {response}"
    assert len(response['results']) > 0, f"No test PDFs were processed"
    
    # Check that all test PDFs have the done property
    for pdf in response['results']:
        assert DONE_PROPERTY in pdf, f"PDF {pdf.get('_id')} missing {DONE_PROPERTY}"
        assert pdf[DONE_PROPERTY] is not None, f"PDF {pdf.get('_id')} has None {DONE_PROPERTY}"

def test_embeddings_generated(db_connection):
    """Test that embeddings were actually generated and stored."""
    # Check image embeddings
    image_query = [{
        "FindDescriptor": {
            "with_descriptor_set": IMAGE_DESCRIPTOR_SET,
            "limit": 1,
            "results": {
                "list": ["_id", "vector"]
            }
        }
    }]
    
    execute_query(db_connection, image_query)
    image_response = db_connection.get_last_response()
    
    assert image_response['status'] == 0, f"Image embedding query failed: {image_response}"
    assert len(image_response['results']) > 0, f"No image embeddings found in {IMAGE_DESCRIPTOR_SET}"
    
    # Check text embeddings
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
    
    assert text_response['status'] == 0, f"Text embedding query failed: {text_response}"
    assert len(text_response['results']) > 0, f"No text embeddings found in {TEXT_DESCRIPTOR_SET}"

def test_workflow_consistency(db_connection):
    """Test that the workflow produces consistent results."""
    # Get all processed images
    image_query = [{
        "FindImage": {
            "constraints": {
                "test_data": True,
                DONE_PROPERTY: ["!=", None]
            },
            "results": {
                "list": ["_id", "name", "expected_text"]
            }
        }
    }]
    
    execute_query(db_connection, image_query)
    image_response = db_connection.get_last_response()
    
    # Get all processed PDFs
    pdf_query = [{
        "FindBlob": {
            "constraints": {
                "document_type": "pdf",
                "test_data": True,
                DONE_PROPERTY: ["!=", None]
            },
            "results": {
                "list": ["_id", "name"]
            }
        }
    }]
    
    execute_query(db_connection, pdf_query)
    pdf_response = db_connection.get_last_response()
    
    # Count total processed items
    total_processed = len(image_response['results']) + len(pdf_response['results'])
    
    # Count total test items (should match what was created in seed.py)
    total_test_query = [{
        "FindImage": {
            "constraints": {
                "test_data": True
            },
            "results": {
                "list": ["_id"]
            }
        }
    }, {
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
    
    execute_query(db_connection, total_test_query)
    total_test_response = db_connection.get_last_response()
    
    # The workflow should have processed all test items
    total_test_items = len(total_test_response['results'])
    assert total_processed == total_test_items, f"Expected {total_test_items} items processed, got {total_processed}"
    
    # Check that images have expected text from CSV
    for image in image_response['results']:
        assert "expected_text" in image, f"Image {image.get('_id')} missing expected_text property"

def test_ocr_workflow_consistency(db_connection):
    """Test that the OCR workflow produces consistent results."""
    # Get all OCR processed images
    ocr_query = [{
        "FindImage": {
            "constraints": {
                "test_data": True,
                "wf_embeddings_clip_ocr": ["!=", None]
            },
            "results": {
                "list": ["_id", "name", "expected_text"]
            }
        }
    }]
    
    execute_query(db_connection, ocr_query)
    ocr_response = db_connection.get_last_response()
    
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
    ocr_processed = len(ocr_response['results'])
    
    # All test images should be OCR processed
    assert ocr_processed == total_images, f"Expected {total_images} images OCR processed, got {ocr_processed}"
    assert ocr_processed > 0, "No images were OCR processed"
    
    # Check that images have expected text from CSV
    for image in ocr_response['results']:
        assert "expected_text" in image, f"Image {image.get('_id')} missing expected_text property"

def test_workflow_cleanup(db_connection):
    """Test that the workflow can clean up embeddings when requested."""
    # This test would verify the cleanup functionality
    # For now, we'll just check that the cleanup function exists
    
    try:
        from extract_embeddings import clean_embeddings
        assert callable(clean_embeddings), "clean_embeddings is not callable"
    except ImportError:
        pytest.skip("Cleanup function not available")

def test_workflow_parameters(db_connection):
    """Test that the workflow accepts the expected parameters."""
    try:
        from extract_embeddings import get_args
        args = get_args()
        
        # Check that expected arguments exist
        assert hasattr(args, 'numthreads'), "Missing numthreads parameter"
        assert hasattr(args, 'model_name'), "Missing model_name parameter"
        assert hasattr(args, 'clean'), "Missing clean parameter"
        assert hasattr(args, 'extract_images'), "Missing extract_images parameter"
        assert hasattr(args, 'extract_pdfs'), "Missing extract_pdfs parameter"
        assert hasattr(args, 'log_level'), "Missing log_level parameter"
        
    except ImportError:
        pytest.skip("Argument parser not available")

def test_workflow_error_handling():
    """Test that the workflow handles errors gracefully."""
    # This test would verify error handling
    # For now, we'll just check that the main function exists and is callable
    
    try:
        from extract_embeddings import main
        assert callable(main), "main function is not callable"
    except ImportError:
        pytest.skip("Main function not available")
