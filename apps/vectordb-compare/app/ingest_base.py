"""
Base classes and shared utilities for ingestion engines.
"""

import os
import time
from abc import ABC, abstractmethod
from collections import defaultdict

import utils as u


class IngestionEngine(ABC):
    """Abstract base class for ingestion engines."""

    def __init__(self, engine_name):
        self.engine_name = engine_name
        self.timing_data = defaultdict(float)

    @abstractmethod
    def ingest_data(self, params):
        """Ingest data for this engine."""
        pass

    def get_dataset(self, source, size):
        """Get dataset based on source and size."""
        if source == "deepimage96":
            return u.DatasetDeepImage96(max=size).dataset
        elif source == "yfcc100m":
            return u.DatasetYFCC100M(max=size).dataset
        else:
            raise ValueError(f"Unknown source: {source}")

    def get_sizes_to_process(self, minimal=False):
        """Get list of sizes to process."""
        if minimal:
            return ["1k"]
        return ["1k", "10k", "100k", "1m"]

    def size_to_count(self, size_str):
        """Convert size string to count."""
        size_map = {"1k": 1000, "10k": 10000, "100k": 100000, "1m": 1000000}
        return size_map.get(size_str)

    def log_progress(self, message):
        """Log progress message."""
        print(message)

    def record_timing(self, size_str, duration):
        """Record timing for a specific dataset size."""
        self.timing_data[size_str] = duration

    def get_timing_data(self):
        """Get collected timing data."""
        return dict(self.timing_data)
