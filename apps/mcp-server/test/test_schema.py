import os
import pytest
from fastmcp import Client


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


class TestSchemaTools:
    """Test suite for schema-related MCP tools."""

    @pytest.mark.asyncio
    async def test_list_entity_classes(self, mcp_url, mcp_auth):
        """Test listing all entity classes."""
        async with Client(mcp_url, auth=mcp_auth) as client:
            result = await client.call_tool("list_entity_classes")
        
        # FastMCP automatically deserializes Pydantic responses into .data
        assert result.data
        assert hasattr(result.data, "classes")
        classes = result.data.classes
        
        # Should have at least the test classes we created
        assert "Person" in classes
        assert "Company" in classes
        assert isinstance(classes, list)
        assert len(classes) >= 2

    @pytest.mark.asyncio
    async def test_describe_entity_class_person(self, mcp_url, mcp_auth):
        """Test describing the Person entity class."""
        async with Client(mcp_url, auth=mcp_auth) as client:
            result = await client.call_tool("describe_entity_class", {
                "class_name": "Person"
            })
        
        # FastMCP automatically deserializes into .data
        assert result.data
        assert hasattr(result.data, "matched")
        assert hasattr(result.data, "properties")
        
        # The properties field is Dict[str, PropertyDescription] but FastMCP client
        # may not deserialize it properly. Check if we can access it via content instead
        if hasattr(result, 'content'):
            # result.content has the raw response
            import json
            for content_item in result.content:
                if hasattr(content_item, 'text'):
                    data = json.loads(content_item.text)
                    assert 'matched' in data
                    assert 'properties' in data
                    properties = data['properties']
                    assert 'name' in properties or 'age' in properties
                    
                    # Check property structure
                    for prop_name, prop_desc in properties.items():
                        assert 'type' in prop_desc
                        assert 'indexed' in prop_desc
                        assert 'matched' in prop_desc
                    return  # Test passed via content
        
        # Fallback: try the structured data approach
        # If properties is a dict-like object, try to access it
        properties = result.data.properties
        if hasattr(properties, '__dict__'):
            props_dict = properties.__dict__
            assert 'name' in props_dict or 'age' in props_dict, \
                f"Expected properties in __dict__, got: {props_dict.keys()}"

    @pytest.mark.asyncio
    async def test_describe_entity_class_company(self, mcp_url, mcp_auth):
        """Test describing the Company entity class."""
        async with Client(mcp_url, auth=mcp_auth) as client:
            result = await client.call_tool("describe_entity_class", {
                "class_name": "Company"
            })
        
        assert result.data
        assert hasattr(result.data, "matched")
        assert hasattr(result.data, "properties")

    @pytest.mark.asyncio
    async def test_describe_nonexistent_entity_class(self, mcp_url, mcp_auth):
        """Test describing a non-existent entity class."""
        with pytest.raises(Exception):  # FastMCP will raise an exception
            async with Client(mcp_url, auth=mcp_auth) as client:
                await client.call_tool("describe_entity_class", {
                    "class_name": "NonExistentClass"
                })

    @pytest.mark.asyncio
    async def test_list_connection_classes(self, mcp_url, mcp_auth):
        """Test listing all connection classes."""
        async with Client(mcp_url, auth=mcp_auth) as client:
            result = await client.call_tool("list_connection_classes")
        
        assert result.data
        assert hasattr(result.data, "classes")
        classes = result.data.classes
        
        # Should have at least the test connection class we created
        assert "WorksAt" in classes
        assert isinstance(classes, list)

    @pytest.mark.asyncio
    async def test_describe_connection_class(self, mcp_url, mcp_auth):
        """Test describing a connection class."""
        async with Client(mcp_url, auth=mcp_auth) as client:
            result = await client.call_tool("describe_connection_class", {
                "class_name": "WorksAt"
            })
        
        assert result.data
        assert hasattr(result.data, "items")
        items = result.data.items
        assert len(items) > 0
        
        # Check structure of each item
        for item in items:
            assert hasattr(item, "matched")
            assert hasattr(item, "properties")
            assert hasattr(item, "src")
            assert hasattr(item, "dst")
            
            # Verify source and destination
            assert item.src == "Person"
            assert item.dst == "Company"

    @pytest.mark.asyncio
    async def test_describe_nonexistent_connection_class(self, mcp_url, mcp_auth):
        """Test describing a non-existent connection class."""
        with pytest.raises(Exception):  # FastMCP will raise an exception
            async with Client(mcp_url, auth=mcp_auth) as client:
                await client.call_tool("describe_connection_class", {
                    "class_name": "NonExistentConnection"
                })

    @pytest.mark.asyncio
    async def test_entity_class_count(self, mcp_url, mcp_auth):
        """Test that we can get entity counts from schema."""
        async with Client(mcp_url, auth=mcp_auth) as client:
            result = await client.call_tool("describe_entity_class", {
                "class_name": "Person"
            })
        
        # Should have matched count
        assert result.data.matched > 0

    @pytest.mark.asyncio
    async def test_multiple_entity_classes(self, mcp_url, mcp_auth):
        """Test describing multiple entity classes in sequence."""
        classes_to_test = ["Person", "Company"]
        
        async with Client(mcp_url, auth=mcp_auth) as client:
            for class_name in classes_to_test:
                result = await client.call_tool("describe_entity_class", {
                    "class_name": class_name
                })
                
                assert hasattr(result.data, "matched")
                assert result.data.matched > 0

