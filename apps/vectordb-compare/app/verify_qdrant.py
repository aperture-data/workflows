"""
Qdrant Ingestion Verification Engine
"""

from verify_base import VerificationEngine
import utils as u


class QdrantVerificationEngine(VerificationEngine):
    """Qdrant ingestion verification engine."""

    def __init__(self):
        super().__init__("qd")

    def verify_ingestion(self, collection_name, dims, expected_elements, **kwargs):
        """Verify Qdrant ingestion."""
        client = u.create_connector_qdrant()

        collection = client.get_collection(collection_name)
        elements = collection.points_count

        client.close()

        success = elements == expected_elements
        return self.log_result(collection_name, expected_elements, elements, success)


def verify_qdrant(collection_name, dims, expected_elements):
    """Entry point for Qdrant ingestion verification."""
    engine = QdrantVerificationEngine()
    return engine.verify_ingestion(collection_name, dims, expected_elements)
