import os
from typing import List, Annotated

from pydantic import BaseModel, Field

from aperturedb.Descriptors import Descriptors

from shared import logger, args, connection_pool
from decorators import declare_mcp_tool
from embeddings import BatchEmbedder, DEFAULT_MODEL

# from fastmcp.prompts.prompt import Message, PromptMessage, TextContent


embedder = BatchEmbedder(model_spec=DEFAULT_MODEL)


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


class ImageDocument(BaseModel):
    doc_id: Annotated[str, Field(
        description="The unique identifier for the image document")]
    thumbnail: Annotated[str, Field(
        description="JPEG thumbnail image of the document")]
    properties: Annotated[dict, Field(
        description="A dictionary of properties associated with the image document")]


class FindSimilarImagesResponse(BaseModel):
    documents: Annotated[List[ImageDocument], Field(
        description="A list of images similar to the query text")]


@declare_mcp_tool
def find_similar_documents(query: Annotated[str, Field(description="The query text to find similar documents for")],
                           k: Annotated[int, Field(
                               description="The maximum number of documents to return")] = 5,
                           descriptor_set: Annotated[str, Field(
                               description="The descriptor set to use for finding similar images")] = args.input,
                           ) -> FindSimilarDocumentsResponse:
    """Find documents that are similar to a given text string"""
    if not descriptor_set:
        raise ValueError(
            "Descriptor set is required. Please provide a valid descriptor set name.")
    embedding = embedder.embed_query(query)
    with connection_pool.get_connection() as client:
        entities = Descriptors(client)
        entities.find_similar(
            set=descriptor_set,
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


@declare_mcp_tool
def find_similar_images(query: Annotated[str, Field(description="The query text to find similar images for")],
                        k: Annotated[int, Field(
                            description="The maximum number of documents to return")] = 5,
                        descriptor_set: Annotated[str, Field(
                            description="The descriptor set to use for finding similar images")] = args.input,
                        ) -> FindSimilarDocumentsResponse:
    """Find documents that are similar to a given text string"""
    if not descriptor_set:
        raise ValueError(
            "Descriptor set is required. Please provide a valid descriptor set name.")
    embedding = embedder.embed_query(query)
    query = [
        {
            "FindDescriptor": {
                "set": descriptor_set,
                "vector": embedding,
                "k_neighbors": k,
                "_ref": 1,
            }
        },
        {
            "FindImage": {
                "is_connected_to": {
                    "ref": 1,
                },
                "uniqueid": True,
                "results": {
                    "all_properties": True,
                },
                "as_format": "jpg",
                "operations": [
                    {
                        "type": "resize",
                        "width": 256,
                    }
                ]
            }
        },
    ]

    response, blobs = connection_pool.query(query)

    def to_image_document(e, blob):
        return ImageDocument(
            doc_id=e["uniqueid"],
            thumbnail=ImageContent(
                type="image",
                data=base64.b64encode(blob).decode("utf-8"),
                mimeType="image/jpeg"
            ),
            properties=e["properties"]
        )

    if not response or not blobs:
        logger.error(f"No similar images found for query: {query}")
        return FindSimilarImagesResponse(documents=[])
    if "FindDescriptor" not in response[0]:
        logger.error(f"Unexpected response format: {response}")
        return FindSimilarImagesResponse(documents=[])
    if not response[0]["FindDescriptor"].get("entities"):
        logger.error(f"No entities found in response: {response}")
        return FindSimilarImagesResponse(documents=[])
    entities = response[0]["FindDescriptor"]["entities"]
    assert len(entities) == len(
        blobs), "Mismatch between entities and blobs length"

    logger.info(
        f"Found {len(entities)} similar images for query: {query} (k={k})")

    return FindSimilarDocumentsResponse(documents=[
        to_image_document(e, b) for e, b in zip(entities, blobs)])


class DescriptorSet(BaseModel):
    name: Annotated[str, Field(description="The name of the descriptor set")]
    count: Annotated[int, Field(
        description="The number of descriptors in the set")]


class DescriptorSetsResponse(BaseModel):
    sets: Annotated[List[DescriptorSet], Field(
        description="A list of available descriptor sets")]


@declare_mcp_tool
def list_descriptor_sets() -> DescriptorSetsResponse:
    """List all available descriptor sets"""
    query = [
        {
            "FindDescriptorSet": {
                "results": {
                    "list": ["_name", "_count"],
                }
            }
        }
    ]
    response, _ = connection_pool.query(query)
    if not response or not response[0].get("FindDescriptorSet"):
        logger.error("No descriptor sets found or unexpected response format.")
        return []
    if "entities" not in response[0]["FindDescriptorSet"]:
        logger.error("No entities found in descriptor set response.")
        return []
    entities = response[0]["FindDescriptorSet"].get("entities", [])
    results = DescriptorSetsResponse(sets=[
        DescriptorSet(
            name=e["_name"],
            count=e["_count"]
        ) for e in entities])
    return results
