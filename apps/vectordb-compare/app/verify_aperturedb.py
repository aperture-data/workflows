"""
ApertureDB Ingestion Verification Engine
"""

from verify_base import VerificationEngine

# ApertureDB imports - only imported when this module is used
from aperturedb import CommonLibrary


class ApertureDBVerificationEngine(VerificationEngine):
    """ApertureDB ingestion verification engine."""

    def __init__(self):
        super().__init__("adb")

    def verify_ingestion(self, collection_name, dims, expected_elements, **kwargs):
        """Verify ApertureDB ingestion."""
        db = CommonLibrary.create_connector()

        response, _ = db.query([{
            "FindDescriptorSet": {
                "with_name": collection_name,
                "results": {
                    "all_properties": True,
                }
            }
        }])

        elements = response[0]["FindDescriptorSet"]["entities"][0]["_count"]
        success = elements == expected_elements

        return self.log_result(collection_name, expected_elements, elements, success)


def verify_aperturedb(set_name, dims, expected_elements):
    """Entry point for ApertureDB ingestion verification."""
    engine = ApertureDBVerificationEngine()
    return engine.verify_ingestion(set_name, dims, expected_elements)
