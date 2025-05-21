# from mcp.server.fastmcp import FastMCP
from mcp.server import Server
from fastapi import FastAPI
from typing import List
from pydantic import BaseModel
from embeddings import BatchEmbedder, DEFAULT_MODEL
from aperturedb import Descriptors
import os
import logging
import asyncio

app = FastAPI(port=8002)
# Create an MCP server
mcp = Server(app)
#     name="ApertureDB",
#     port=8002,
#     log_level="DEBUG",
# )

embedder = BatchEmbedder(model_spec=DEFAULT_MODEL)

descriptor_set = os.environ.get("WF_INPUT", "aperturedata")

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@mcp.tool()
def test() -> str:
    """Test the server"""
    logger.info("Test tool called")
    return "Hello, world!"


class FindSimilarDocumentsRequest(BaseModel):
    query: str
    k: int = 5


class Document(BaseModel):
    doc_id: str
    url: str
    text: str


class FindSimilarDocumentsResponse(BaseModel):
    documents: List[Document]


@mcp.tool()
def find_similar_documents(req: FindSimilarDocumentsRequest) -> FindSimilarDocumentsResponse:
    """Find documents that are similar to a given text string"""
    logger.info(f"Finding similar documents for query: {req.query}")
    embedding = embedder.embed_query(req.query)
    entities = Descriptors.find_similar(
        set=descriptor_set,
        vector=embedding,
        k_neighbors=req.k,
        results={"list": ["uniqueid", "url", "text"]}
    )
    logger.info(f"Found {len(entities)} similar documents")
    return FindSimilarDocumentsResponse(documents=[
        Document(doc_id=e["uniqueid"], url=e["url"], text=e["text"])
        for e in entities
    ])


async def main():
    logging.basicConfig(level=logging.DEBUG)
    logger.info(
        f"Starting ApertureDB MCP server {mcp.name} - {await mcp.list_tools()}")
    try:
        await mcp.run()
        # pass
    except Exception as e:
        logger.error(f"Error starting server: {e}")
        raise e
    logging.info("Server stopped")

if __name__ == "__main__":
    asyncio.run(main())
