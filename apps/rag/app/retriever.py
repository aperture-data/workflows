from dataclasses import dataclass
from typing import List, Dict
from aperturedb.Descriptors import Descriptors


@dataclass
class Retriever:
    embeddings: "BatchEmbedder"
    descriptor_set: str
    search_type: str  # "mmr" or "similarity"
    k: int
    fetch_k: int
    client: "Connector"

    def invoke(query: str) -> List[Dict]:
        descriptors = Decriptors(self.client)
        embedding = self.embeddings.embed_query(query)
        if self.search_type == "mmr":
            results = descriptors.find_similar_mmr(
                embedding,
                self.k,
                self.fetch_k,
            )
        elif self.search_type == "similarity":
            results = descriptors.find_similar(
                embedding,
                self.k,
            )
        else:
            raise ValueError(
                f"Invalid search type: {self.search_type}. Must be 'mmr' or 'similarity'.")
        return results

    def count(self):
        query = [
            {"FindDescriptorSet": {
                "with_name": self.descriptor_set,
                "counts": True
            }}
        ]
        response, _ = self.client.query(query)
        if not response or not response[0].get("FindDescriptorSet"):
            return 0
        if "entities" not in response[0]["FindDescriptorSet"]:
            return 0
        if len(response[0]["FindDescriptorSet"]["entities"]) == 0:
            return 0

        return response[0]["FindDescriptorSet"]["entities"][0].get("_count", 0)
