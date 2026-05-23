import os
import pytest
from fastmcp import Client
import pytest_asyncio


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

@pytest_asyncio.fixture
async def client(mcp_url, mcp_auth):
    """Get a FastMCP client."""
    async with Client(mcp_url, auth=mcp_auth, timeout=30) as aclient:
        yield aclient


class TestFindSimilarTools:
    """Test suite for find_similar MCP tools."""

    @pytest.mark.asyncio
    async def test_list_descriptor_sets(self, client):
        """Test listing all descriptor sets."""
        result = await client.call_tool("list_descriptor_sets")

        # With FastMCP, structured responses are in .data
        assert result.data
        assert result.data.sets
        sets = result.data.sets

        # Should have at least the test descriptor set we created
        assert len(sets) > 0
        assert any(s.name == "TestText_0" for s in sets)

        # Check structure of each set
        for descriptor_set in sets:
            assert hasattr(descriptor_set, "name")
            assert hasattr(descriptor_set, "count")
            assert descriptor_set.count > 0

    @pytest.mark.asyncio
    async def test_find_similar_documents_basic(self, client):
        """Test basic find similar documents functionality."""
        result = await client.call_tool("find_similar_documents", {
            "query": "people walking in nature",
            "k": 3,
            "descriptor_set": "TestText_0"
        })

        # Structured response in .data
        assert result.data
        assert result.data.documents
        documents = result.data.documents

        # Should return k documents
        assert len(documents) <= 3
        assert len(documents) > 0

        # Check structure of each document
        for doc in documents:
            assert hasattr(doc, "doc_id")
            assert hasattr(doc, "url")
            assert hasattr(doc, "text")
            assert isinstance(doc.text, str)
            assert len(doc.text) > 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize("query_text", [
        "mountain landscape",
        "cat on windowsill",
        "busy city street",
        "child playing",
        "family picnic"
    ])
    async def test_find_similar_documents_various_queries(self, client, query_text):
        """Test find similar with various query texts."""
        result = await client.call_tool("find_similar_documents", {
            "query": query_text,
            "k": 5,
            "descriptor_set": "TestText_0"
        })

        assert result.data
        documents = result.data.documents

        # Should return up to k documents
        assert len(documents) <= 5
        assert len(documents) > 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize("k_value", [1, 3, 5, 10])
    async def test_find_similar_documents_different_k(self, client, k_value):
        """Test find similar with different k values."""
        result = await client.call_tool("find_similar_documents", {
            "query": "outdoor activity",
            "k": k_value,
            "descriptor_set": "TestText_0"
        })

        documents = result.data.documents

        # Should return at most k documents
        assert len(documents) <= k_value

    @pytest.mark.asyncio
    async def test_find_similar_documents_with_default_descriptor_set(self, client):
        """Test find similar using the default descriptor set."""
        # The default is set in docker-compose as WF_INPUT=TestText_0
        result = await client.call_tool("find_similar_documents", {
            "query": "sunset over mountains",
            "k": 3
        })

        assert result.data
        documents = result.data.documents
        assert len(documents) > 0

    @pytest.mark.asyncio
    async def test_find_similar_documents_nonexistent_set(self, client):
        """Test find similar with a non-existent descriptor set."""
        # Check that the call raises ToolError wrapping a ValueError
        with pytest.raises(Exception) as exc_info:
            await client.call_tool("find_similar_documents", {
                "query": "test query",
                "k": 3,
                "descriptor_set": "NonExistentSet"
            })
        ex = exc_info.value
        # Check both type and message
        assert type(ex).__name__ == "ToolError"
        assert "ValueError" in str(ex)


    @pytest.mark.asyncio
    async def test_find_similar_documents_empty_query(self, client):
        """Test find similar with an empty query."""
        # For now, empty queries are allowed
        result = await client.call_tool("find_similar_documents", {
            "query": "",
            "k": 3,
            "descriptor_set": "TestText_0"
        })
        assert result.data

    @pytest.mark.asyncio
    async def test_find_similar_documents_large_k(self, client):
        """Test find similar with k larger than available documents."""
        result = await client.call_tool("find_similar_documents", {
            "query": "test query",
            "k": 1000,
            "descriptor_set": "TestText_0"
        })

        documents = result.data.documents

        # TODO: Decouple this from the seed data
        assert len(documents) == 10  # We seeded exactly 10 documents

    @pytest.mark.asyncio
    async def test_find_similar_documents_verify_uniqueness(self, client):
        """Test that returned documents have unique IDs."""
        result = await client.call_tool("find_similar_documents", {
            "query": "nature scene",
            "k": 5,
            "descriptor_set": "TestText_0"
        })

        documents = result.data.documents

        # Check that all document IDs are unique
        doc_ids = [doc.doc_id for doc in documents]
        assert len(doc_ids) == len(set(doc_ids)), "Document IDs should be unique"

    @pytest.mark.asyncio
    async def test_find_similar_documents_url_format(self, client):
        """Test that returned URLs have correct format."""
        result = await client.call_tool("find_similar_documents", {
            "query": "outdoor scene",
            "k": 5,
            "descriptor_set": "TestText_0"
        })

        documents = result.data.documents

        # Check URL format for each document
        for doc in documents:
            assert doc.url.startswith("https://example.com/doc_")

    @pytest.mark.asyncio
    async def test_multiple_queries_in_sequence(self, client):
        """Test multiple find similar calls in sequence."""
        queries = ["hiking", "cat", "city", "child", "food"]

        for query in queries:
            result = await client.call_tool("find_similar_documents", {
                "query": query,
                "k": 3,
                "descriptor_set": "TestText_0"
            })

            assert result.data
            assert result.data.documents
            assert len(result.data.documents) > 0
