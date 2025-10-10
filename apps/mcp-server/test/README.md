# MCP Server Test Suite

This directory contains the test suite for the MCP Server workflow, which provides MCP (Model Context Protocol) access to ApertureDB.

## Test Structure

The test suite includes:

- **`seed.py`**: Populates ApertureDB with test data including entities, connections, descriptors, and images
- **`test_schema.py`**: Tests for schema-related MCP tools (list/describe entity and connection classes)
- **`test_find_similar.py`**: Tests for find_similar tools (document and image similarity search)
- **`test_resources.py`**: Tests for MCP resources (image fetching with format conversion)

## Test Data

The seed script creates:
- 10 Person entities with name and age properties
- 5 Company entities with name and employees properties
- 5 WorksAt connections between Person and Company entities
- 1 text descriptor set (TestText_0) with 10 descriptors containing sample text
- 10 random test images

## Running Tests

From the `apps/mcp-server` directory:

```bash
./test.sh
```

This will:
1. Start the ApertureDB database (lenz)
2. Run the seed script to populate test data
3. Start the mcp-server workflow
4. Run all pytest tests
5. Clean up containers

## Test Coverage

### Schema Tools (`test_schema.py`)
- List entity classes
- Describe entity classes (Person, Company)
- Handle non-existent entity classes
- List connection classes
- Describe connection classes (WorksAt)
- Verify entity counts
- Test multiple entity classes

### Find Similar Tools (`test_find_similar.py`)
- List available descriptor sets
- Find similar documents with various queries
- Test different k values (number of results)
- Use default descriptor set
- Handle non-existent descriptor sets
- Verify document uniqueness
- Validate URL formats
- Sequential queries

### Image Resources (`test_resources.py`)
- Fetch images in JPG format
- Fetch images in PNG format
- Case-insensitive format handling
- Multiple image fetching
- Handle non-existent image IDs
- Validate image content structure
- Format conversion (same image in different formats)
- Unauthorized access (without auth token)

## Environment Variables

Tests use the following environment variables:
- `MCP_HOST`: MCP server hostname (default: `mcp-server`)
- `MCP_PORT`: MCP server port (default: `8000`)
- `MCP_AUTH_TOKEN`: Bearer token for authentication (default: `test-token-12345`)
- `DB_HOST`: ApertureDB hostname (default: `lenz`)
- `DB_PORT`: ApertureDB port (default: `55551`)
- `DB_USER`: ApertureDB username (default: `admin`)
- `DB_PASS`: ApertureDB password (default: `admin`)

## Dependencies

Test dependencies are specified in `requirements.txt`:
- `aperturedb`: ApertureDB Python client
- `pytest`: Test framework
- `pytest-asyncio`: Async test support
- `fastmcp`: FastMCP client for MCP protocol communication
- `pillow`: Image processing for validation
- `numpy`: Numerical operations for test data generation

## Test Implementation

Tests use the `fastmcp.Client` to communicate with the MCP server:

```python
from fastmcp import Client

async with Client(mcp_url, auth=auth_token) as client:
    # Call a tool - structured responses are automatically deserialized
    result = await client.call_tool("list_entity_classes")
    classes = result.data.classes  # Direct access to Pydantic model
    
    # Read a resource
    resource = await client.read_resource("aperturedb:/image/jpg/image-id")
    image_content = resource.contents[0]  # ImageContent object
```

This provides idiomatic MCP protocol access rather than making raw HTTP calls.

### Structured Responses

When MCP tools return Pydantic models, `fastmcp.Client` automatically deserializes them:
- Tools returning Pydantic models → access via `result.data.<attribute>`
- No need to parse JSON manually
- Full type safety and IDE completion
- Example: `result.data.classes`, `result.data.documents`, `result.data.sets`

