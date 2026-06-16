"""
Base classes and shared utilities for ingestion verification engines.
"""

import os
from abc import ABC, abstractmethod


class VerificationEngine(ABC):
    """Abstract base class for ingestion verification engines."""

    def __init__(self, engine_name):
        self.engine_name = engine_name

    @abstractmethod
    def verify_ingestion(self, collection_name, dims, expected_elements, **kwargs):
        """Verify ingestion for this engine."""
        pass

    def log_result(self, collection_name, expected, actual, success):
        """Log verification result."""
        if success:
            print(
                f"Verifying {self.engine_name.ljust(3)} {collection_name}... OK")
        else:
            print(
                f"Verifying {self.engine_name.ljust(3)} {collection_name}... FAILED")
            print(f"  Expected: {expected}, Got: {actual}")
        return success
