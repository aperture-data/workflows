"""
Weaviate Ingestion Verification Engine
"""

from verify_base import VerificationEngine
import utils as u


class WeaviateVerificationEngine(VerificationEngine):
    """Weaviate ingestion verification engine."""

    def __init__(self):
        super().__init__("wv")

    def verify_ingestion(self, collection_name, dims, expected_elements, **kwargs):
        """Verify Weaviate ingestion."""
        client = u.create_connector_weaviate()

        collection = client.collections.get(collection_name)
        response = collection.aggregate.over_all(total_count=True)
        elements = response.total_count

        client.close()

        success = elements == expected_elements
        return self.log_result(collection_name, expected_elements, elements, success)


def verify_weaviate(collection_name, dims, expected_elements):
    """Entry point for Weaviate ingestion verification."""
    engine = WeaviateVerificationEngine()
    return engine.verify_ingestion(collection_name, dims, expected_elements)
