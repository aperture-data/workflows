"""
Pinecone Ingestion Verification Engine
"""

from verify_base import VerificationEngine
import utils as u


class PineconeVerificationEngine(VerificationEngine):
    """Pinecone ingestion verification engine."""

    def __init__(self):
        super().__init__("pc")

    def verify_ingestion(self, collection_name, dims, expected_elements, dataset=None, **kwargs):
        """Verify Pinecone ingestion."""
        pc = u.create_connector_pinecone(grpc=False)

        index_name = f"knn-{dataset}"
        index = pc.Index(index_name)

        elements = index.describe_index_stats(
        )["namespaces"][collection_name]["vector_count"]
        success = elements == expected_elements

        return self.log_result(collection_name, expected_elements, elements, success)


def verify_pinecone(dataset, namespace, dims, expected_elements):
    """Entry point for Pinecone ingestion verification."""
    engine = PineconeVerificationEngine()
    return engine.verify_ingestion(namespace, dims, expected_elements, dataset=dataset)
