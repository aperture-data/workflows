import os
from typing import List, Annotated

from pydantic import BaseModel, Field

from aperturedb.Descriptors import Descriptors

from shared import logger, args, connection_pool
from decorators import declare_mcp_tool
from embeddings import BatchEmbedder, DEFAULT_MODEL

# from fastmcp.prompts.prompt import Message, PromptMessage, TextContent


embedder = BatchEmbedder(model_spec=DEFAULT_MODEL)


class FindSimilarDocumentsRequest(BaseModel):
    query: Annotated[str, Field(
        description="The query text to find similar documents for", min_length=1)]
    k: Annotated[int, Field(
        description="The maximum number of documents to return", gt=0)] = 5


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
                           ) -> FindSimilarDocumentsResponse:
    """Find documents that are similar to a given text string"""
    embedding = embedder.embed_query(query)
    with connection_pool.get_connection() as client:
        entities = Descriptors(client)
        entities.find_similar(
            set=args.input,  # TODO: Make this an optional parameter with optional default
            vector=embedding,
            k_neighbors=k,
            results={"list": ["uniqueid", "url", "text"]}
        )
    logger.info(
        f"Found {len(entities)} similar documents for query: {query} (k={k})")
    return FindSimilarDocumentsResponse(documents=[
        Document(doc_id=e["uniqueid"], url=e["url"], text=e["text"])
        for e in entities
    ])


# @declare_mcp_prompt(name="answer_question_about_aperturedb",
#             description="Answers questions about ApertureDB using the document corpus.")
# def answer_question_about_aperturedb(req: PromptRequest) -> str:
#     """Answer a user’s question about ApertureDB by searching the document corpus."""

#     query = req.prompt
#     logger.info(f"Prompt received: {query}")

#     # Call the tool directly
#     results = find_similar_documents(
#         FindSimilarDocumentsRequest(query=query, k=5)
#     )

#     # Simple RAG summarization (you could use Claude here!)
#     if not results.documents:
#         return "Sorry, I couldn't find any relevant documents."

#     context = "\n\n".join(f"- {doc.text}" for doc in results.documents)
#     answer = f"""Here are some relevant details about your question:

# {context}

# Please let me know if you'd like to explore further."""

#     return answer
