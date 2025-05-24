import logging
import os
from typing import List

from pydantic import BaseModel

from aperturedb import Descriptors

from decorators import declare_mcp_tool
from embeddings import BatchEmbedder, DEFAULT_MODEL

logger = logging.getLogger(__name__)

embedder = BatchEmbedder(model_spec=DEFAULT_MODEL)

descriptor_set = os.environ.get("WF_INPUT", "aperturedata")


class FindSimilarDocumentsRequest(BaseModel):
    query: str
    k: int = 5


class Document(BaseModel):
    doc_id: str
    url: str
    text: str


class FindSimilarDocumentsResponse(BaseModel):
    documents: List[Document]


@declare_mcp_tool
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
