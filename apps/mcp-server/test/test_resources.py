import os
import pytest
from fastmcp import Client
from PIL import Image
from io import BytesIO
import base64


@pytest.fixture(scope="session")
def mcp_url():
    """Get the MCP server URL."""
    host = os.getenv("MCP_HOST", "mcp-server")
    port = os.getenv("MCP_PORT", "8000")
    return f"http://{host}:{port}/mcp"


@pytest.fixture(scope="session")
def mcp_auth():
    """Get the MCP auth token."""
    return os.getenv("MCP_AUTH_TOKEN", "test")


@pytest.fixture(scope="session")
def adb_client():
    """Create an ApertureDB client to query for image IDs."""
    from aperturedb.Connector import Connector
    
    def str_to_bool(s):
        return s.lower() in ["true", "1", "yes", "y"]
    
    DB_HOST = os.getenv("DB_HOST", "lenz")
    DB_PORT = int(os.getenv("DB_PORT", "55551"))
    DB_USER = os.getenv("DB_USER", "admin")
    DB_PASS = os.getenv("DB_PASS", "admin")
    CA_CERT = os.getenv("CA_CERT", None)
    USE_SSL = str_to_bool(os.getenv("USE_SSL", "true"))
    
    client = Connector(
        host=DB_HOST, 
        user=DB_USER, 
        port=DB_PORT, 
        password=DB_PASS, 
        ca_cert=CA_CERT, 
        use_ssl=USE_SSL
    )
    yield client


def get_image_ids(adb_client, limit=5):
    """Get some image IDs from the database."""
    from aperturedb.CommonLibrary import execute_query
    
    query = [{
        "FindImage": {
            "results": {
                "list": ["_uniqueid"]
            },
            "limit": limit
        }
    }]
    
    status, response, _ = execute_query(adb_client, query)
    assert status == 0
    
    if not response or not response[0].get("FindImage", {}).get("entities"):
        return []
    
    return [e["_uniqueid"] for e in response[0]["FindImage"]["entities"]]


def validate_image_content(blob_content, expected_format):
    """Validate that the blob content is valid image data in the expected format."""
    # BlobResourceContents has: uri, mimeType, blob (base64 string)
    assert hasattr(blob_content, "mimeType"), "Missing 'mimeType' attribute"
    assert hasattr(blob_content, "blob"), "Missing 'blob' attribute (base64 data)"
    
    # Check MIME type matches expected format
    expected_mime = f"image/{expected_format}" if expected_format == "png" else "image/jpeg"
    assert blob_content.mimeType == expected_mime, \
        f"Expected MIME type {expected_mime}, got {blob_content.mimeType}"
    
    # Decode base64 blob data
    image_data = base64.b64decode(blob_content.blob)
    
    # Try to open with PIL to verify it's valid image data
    img = Image.open(BytesIO(image_data))
    actual_format = img.format.upper()
    
    # Normalize format names
    expected = expected_format.upper()
    if expected == "JPG":
        expected = "JPEG"
    
    assert actual_format == expected, f"Expected image format {expected}, got {actual_format}"
    return True


class TestImageResources:
    """Test suite for image resource endpoints."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("format", ["jpg", "png"])
    async def test_get_image(self, mcp_url, mcp_auth, adb_client, format):
        """Test fetching an image in the specified format."""
        image_ids = get_image_ids(adb_client, limit=1)
        
        if not image_ids:
            pytest.skip("No images available for testing")
        
        image_id = image_ids[0]
        # New URI structure: separate resources per format
        uri = f"aperturedb://image/{format}/{image_id}"
        
        async with Client(mcp_url, auth=mcp_auth) as client:
            result = await client.read_resource(uri)
        
        # result is a list of resource contents (BlobResourceContents for bytes)
        assert result, "Resource returned no content"
        assert len(result) > 0, "Resource returned empty content list"
        
        # Validate the image content
        blob_content = result[0]
        validate_image_content(blob_content, format)