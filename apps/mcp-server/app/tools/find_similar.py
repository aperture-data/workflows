import os
from typing import List, Annotated

from pydantic import BaseModel, Field
import numpy as np
import base64

from aperturedb.Descriptors import Descriptors

from shared import logger, args, connection_pool
from decorators import declare_mcp_tool
from embeddings import Embedder
from mcp.types import ImageContent


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


def get_embedder(descriptor_set: str) -> Embedder:
    """Get an instance of Embedder for the specified descriptor set.
    THis ensures that we embed the input text or image using the correct model.
    """
    if not descriptor_set:
        raise ValueError(
            "Descriptor set is required. Please provide a valid descriptor set name.")

    with connection_pool.get_connection() as client:
        embedder = Embedder.from_descriptor_set(
            client=client,
            descriptor_set=descriptor_set,
        )
    return embedder


def find_text_documents(descriptor_set: str, embedding: np.ndarray, k: int) -> FindSimilarDocumentsResponse:
    """Find text documents similar to the given embedding."""
    with connection_pool.get_connection() as client:
        entities = Descriptors(client)
        entities.find_similar(
            set=descriptor_set,
            vector=embedding.tobytes(),
            k_neighbors=k,
            constraints={"text": ["!=", None]},  # Only text documents
            results={"list": ["uniqueid", "url", "text"]}
        )

    logger.info(
        f"Found {len(entities)} similar documents (k={k})")
    return FindSimilarDocumentsResponse(documents=[
        Document(doc_id=e["uniqueid"], url=e["url"], text=e["text"])
        for e in entities
    ])


@declare_mcp_tool
def find_similar_documents_for_text(
    query: Annotated[str, Field(description="The query text to find similar documents for")],
    k: Annotated[int, Field(
        description="The maximum number of documents to return")] = 5,
    descriptor_set: Annotated[str, Field(
        description="The descriptor set to use for finding similar documents")] = args.input,
) -> FindSimilarDocumentsResponse:
    """Find text documents that are similar to a given text query"""
    embedder = get_embedder(descriptor_set)
    embedding = embedder.embed_text(query)
    return find_text_documents(descriptor_set=descriptor_set,
                               embedding=embedding,
                               k=k)


def find_images(descriptor_set: str, embedding: np.ndarray, k: int) -> FindSimilarImagesResponse:
    """Find images similar to the given embedding."""
    query = [
        {
            "FindDescriptor": {
                "set": descriptor_set,
                "k_neighbors": k,
                "_ref": 1,
            }
        },
        {
            "FindImage": {
                "is_connected_to": {
                    "ref": 1,
                },
                "uniqueids": True,
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
    input_blobs = [embedding.tobytes()]

    status, response, blobs = connection_pool.execute_query(query, input_blobs)
    assert status == 0, f"Error executing query: {response}"

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
        f"Found {len(entities)} similar images (k={k})")

    return FindSimilarImagesResponse(documents=[
        to_image_document(e, b) for e, b in zip(entities, blobs)])


@declare_mcp_tool
def find_similar_images_for_text(
    query: Annotated[str, Field(description="The query text to find similar images for")],
    k: Annotated[int, Field(
        description="The maximum number of documents to return")] = 5,
    descriptor_set: Annotated[str, Field(
        description="The descriptor set to use for finding similar images")] = args.input,
) -> FindSimilarImagesResponse:
    """Find images that are similar to a given text query"""
    embedder = get_embedder(descriptor_set)
    embedding = embedder.embed_text(query)
    return find_images(descriptor_set=descriptor_set,
                       embedding=embedding,
                       k=k)


class DescriptorSet(BaseModel):
    name: Annotated[str, Field(description="The name of the descriptor set")]
    count: Annotated[int, Field(
        description="The total number of descriptors in the set")]
    documents: Annotated[int, Field(
        description="The total number of text documents associated with the descriptor set")] = 0
    images: Annotated[int, Field(
        description="The total number of images associated with the descriptor set")] = 0


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
                    "list": [
                        "_name",
                        "_count",
                        "_uniqueid"
                    ]
                }
            }
        },
        {
            "FindDescriptor": {
                "results": {
                    "count": True,
                    "group": [
                        "_set_name"
                    ]
                },
                "constraints": {
                    "text": [
                        "!=",
                        None
                    ]
                }
            }
        },
        {
            "FindImage": {
                "_ref": 1
            }
        },
        {
            "FindDescriptor": {
                "results": {
                    "count": True,
                    "group": [
                        "_set_name"
                    ]
                },
                "is_connected_to": {
                    "ref": 1
                }
            }
        }
    ]
    response, _ = connection_pool.query(query)
    if not response or not response[0].get("FindDescriptorSet"):
        logger.error("No descriptor sets found or unexpected response format.")
        return DescriptorSetsResponse(sets=[])
    if "entities" not in response[0]["FindDescriptorSet"]:
        logger.error("No entities found in descriptor set response.")
        return DescriptorSetsResponse(sets=[])
    entities = response[0]["FindDescriptorSet"].get("entities", [])
    results = DescriptorSetsResponse(sets=[
        DescriptorSet(
            name=e["_name"],
            count=e["_count"]
        ) for e in entities])

    # Copy the count of text documents from the second response
    if len(response) > 1 and "FindDescriptor" in response[1] and "groups" in response[1]["FindDescriptor"]:
        logger.info(
            f"Processing text document counts for descriptor sets: {response[1]['FindDescriptor']['groups']}")
        for group in response[1]["FindDescriptor"]["groups"]:
            set_name = group["_set_name"]
            count = group["_group_count"]
            for ds in results.sets:
                if ds.name == set_name:
                    ds.documents = count

    # Copy the count of images from the fourth response
    if len(response) > 3 and "FindDescriptor" in response[3] and "groups" in response[3]["FindDescriptor"]:
        logger.info(
            f"Processing image counts for descriptor sets: {response[3]['FindDescriptor']['groups']}")
        for group in response[3]["FindDescriptor"]["groups"]:
            set_name = group["_set_name"]
            count = group["_group_count"]
            for ds in results.sets:
                if ds.name == set_name:
                    ds.images = count

    return results
