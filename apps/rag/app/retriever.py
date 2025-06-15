from dataclasses import dataclass
from typing import List, Dict, Optional
from aperturedb.Descriptors import Descriptors
from aperturedb.CommonLibrary import execute_query
import logging

logger = logging.getLogger(__name__)

ID_KEY = "uniqueid"
URL_KEY = "lc_url"
PAGE_CONTENT_KEY = "text"


@dataclass
class Document:
    id: str
    url: str
    page_content: str
    title: Optional[str] = None

    def __init__(self, data):
        self.id = data.get(ID_KEY, "")
        self.url = data.get(URL_KEY, "")
        self.page_content = data.get(PAGE_CONTENT_KEY, "")
        self.title = data.get("title", None)

    def to_json(self):
        return {"id": self.id, "url": self.url, "content": self.page_content, "title": self.title}


@dataclass
class Retriever:
    embeddings: "BatchEmbedder"
    descriptor_set: str
    search_type: str  # "mmr" or "similarity"
    k: int
    fetch_k: int
    client: "Connector"

    def invoke(self, query: str) -> List[Document]:
        descriptors = Descriptors(self.client)
        embedding = self.embeddings.embed_query(query)
        if self.search_type == "mmr":
            descriptors.find_similar_mmr(
                set=self.descriptor_set,
                vector=embedding,
                k_neighbors=self.k,
                fetch_k=self.fetch_k,
            )
        elif self.search_type == "similarity":
            descriptors.find_similar(
                set=self.descriptor_set,
                vector=embedding,
                k_neighbors=self.k,
            )
        else:
            raise ValueError(
                f"Invalid search type: {self.search_type}. Must be 'mmr' or 'similarity'.")

        results = [Document(doc) for doc in descriptors.response]
        logger.info(
            f"Retrieved {len(results)} documents for query: {query}")
        logger.debug(
            f"Results: {results}")
        return results

    def count(self):
        query = [
            {"FindDescriptorSet": {
                "with_name": self.descriptor_set,
                "counts": True
            }}
        ]
        status, response, _ = execute_query(self.client, query)
        if status != 0:
            logger.error(f"Error executing count query: {response}")
            return 0
        if not response or not response[0].get("FindDescriptorSet"):
            return 0
        if "entities" not in response[0]["FindDescriptorSet"]:
            return 0
        if len(response[0]["FindDescriptorSet"]["entities"]) == 0:
            return 0

        return response[0]["FindDescriptorSet"]["entities"][0].get("_count", 0)
