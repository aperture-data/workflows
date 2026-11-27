import os
from typing import List, Annotated

from pydantic import BaseModel, Field

from aperturedb.Descriptors import Descriptors

from shared import logger, args, connection_pool
from decorators import declare_mcp_tool
from aperturedb_workflows.embeddings import Embedder


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

    with connection_pool.get_connection() as client:
        embedder = Embedder.from_existing_descriptor_set(
            client, descriptor_set)

    embedding = embedder.embed_text(query)

    with connection_pool.get_connection() as client:
        entities = Descriptors(client)
        entities.find_similar(
            set=descriptor_set,
            vector=embedding,
            k_neighbors=k,
            results={"list": ["_uniqueid", "url", "text"]}
        )
    logger.info(
        f"Found {len(entities)} similar documents for query: {query} (k={k})")
    return FindSimilarDocumentsResponse(documents=[
        Document(doc_id=e["_uniqueid"], url=e["url"], text=e["text"])
        for e in entities
    ])


@declare_mcp_tool
def find_similar_images(query: Annotated[str,
                                         Field(description="Text to search for")],
                        k: Annotated[int,
                                     Field(description="Maximum images")] = 5,
                        descriptor_set: Annotated[str,
                                                  Field(description="Descriptor class")] = args.input,
                        ) -> FindSimilarImagesResponse:
    """Find images connected to ranked descriptors in the selected class."""
    import base64
    import numpy as np
    from aperturedb.DescriptorEngines import descriptor_search
    from aperturedb.CommonLibrary import execute_query
    if not descriptor_set:
        raise ValueError("A descriptor class is required")
    with connection_pool.get_connection() as client:
        embedder = Embedder.from_existing_descriptor_set(client, descriptor_set)
        embedding = np.asarray(embedder.embed_text(query), dtype=np.float32).tobytes()
        search = descriptor_search(client, descriptor_set)
        status, response, blobs = execute_query(client, [
            {"FindDescriptor": {**search, "k_neighbors": k, "_ref": 1, "uniqueids": True}},
            {"FindImage": {"is_connected_to": {"ref": 1}, "uniqueids": True,
                           "group_by_source": True, "blobs": True, "as_format": "jpg",
                           "results": {"all_properties": True},
                           "operations": [{"type": "resize", "width": 256}]}}], [embedding])
    if status != 0:
        raise RuntimeError(f"Image search failed: {response}")
    images = response[1]["FindImage"].get("entities", {})
    documents = []
    for descriptor in response[0]["FindDescriptor"].get("entities", []):
        for image in images.get(descriptor["_uniqueid"], []):
            documents.append(ImageDocument(doc_id=image["_uniqueid"],
                                           thumbnail=base64.b64encode(
                                               blobs[image["_blob_index"]]).decode("ascii"),
                                           properties={key: value for key, value in image.items() if not key.startswith("_")}))
    return FindSimilarImagesResponse(documents=documents[:k])


class DescriptorSet(BaseModel):
    name: Annotated[str, Field(description="The name of the descriptor set")]
    count: Annotated[int, Field(
        description="The number of descriptors in the set")]


class DescriptorSetsResponse(BaseModel):
    sets: Annotated[List[DescriptorSet], Field(
        description="A list of available descriptor sets")]


@declare_mcp_tool
def list_descriptor_sets() -> DescriptorSetsResponse:
    """List populated descriptor classes whose index metadata identifies an embedder."""
    with connection_pool.get_utils() as utils:
        from aperturedb.ServerCompatibility import preferred_vector_index
        by_class = {}
        for index in utils.get_vector_indexes():
            by_class.setdefault(index["class"], []).append(index)
        sets = []
        for name, indexes in by_class.items():
            index = preferred_vector_index(indexes)
            if not Embedder.check_properties(index.get("metadata", {})):
                continue
            count = utils.count_descriptors_in_set(name)
            if count:
                sets.append(DescriptorSet(name=name, count=count))
    return DescriptorSetsResponse(sets=sets)
