import os
from typing import List, Annotated

from pydantic import BaseModel, Field

from aperturedb import Descriptors
from aperturedb.CommonLibrary import create_connector

from shared import logger
from decorators import declare_mcp_tool
from embeddings import BatchEmbedder, DEFAULT_MODEL


embedder = BatchEmbedder(model_spec=DEFAULT_MODEL)

descriptor_set = os.environ.get("WF_INPUT", "aperturedata")


class FindSimilarDocumentsRequest(BaseModel):
    query: Annotated[str, Field(
        description="The query text to find similar documents for")]
    k: Annotated[int, Field(
        description="The maximum number of documents to return")] = 5


class Document(BaseModel):
    doc_id: Annotated[str, Field(
        description="The unique identifier for the document")]
    url: Annotated[str, Field(
        description="The URL where the document can be accessed")]
    text: Annotated[str, Field(
        description="The text content of the document")]


class FindSimilarDocumentsResponse(BaseModel):
    documents: Annotated[List[Document], Field(
        description="A list of documents similar to the query text")]


@declare_mcp_tool
# def find_similar_documents(req: FindSimilarDocumentsRequest) -> FindSimilarDocumentsResponse:
def find_similar_documents(query: Annotated[str, Field(description="The query text to find similar documents for")],
                           k: Annotated[int, Field(
                               description="The maximum number of documents to return")] = 5,
                           ) -> List[Document]:
    """Find documents that are similar to a given text string"""
    logger.info(f"Finding similar documents for query: {query}")
    embedding = embedder.embed_query(query)
    client = create_connector()
    descriptors = Descriptors(client)
    descriptors.find_similar(
        set=descriptor_set,
        vector=embedding,
        k_neighbors=k,
        results={"list": ["uniqueid", "url", "text"]}
    )
    entities = list(descriptors)
    logger.info(f"Found {len(entities)} similar documents")
    return [
        Document(doc_id=e["uniqueid"], url=e["url"], text=e["text"])
        for e in entities
    ]


logger.info("find_similar_documents tool registered!")
