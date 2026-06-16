"""
Base classes and shared utilities for KNN benchmarking engines.
"""

import logging
import os
import time
import numpy as np
from abc import ABC, abstractmethod

import utils as u
from dbeval import EvalTool


def size_to_str(s):
    """Convert size to string representation."""
    if s == 1000:
        return "1k"
    elif s == 10000:
        return "10k"
    elif s == 100000:
        return "100k"
    elif s == 1000000:
        return "1m"


class KNNEngine(ABC):
    """Abstract base class for KNN benchmark engines."""

    def __init__(self, engine_name):
        self.engine_name = engine_name

    @abstractmethod
    def run_benchmark(self, params):
        """Run the KNN benchmark for this engine."""
        pass

    def setup_eval_tool(self, params):
        """Setup the evaluation tool."""
        eval_name = os.path.join(params.output_folder, "knn_comp")
        return EvalTool.EvalTool(eval_name)

    def log_percentiles(self, times):
        """Log timing percentiles."""
        u.print_percentiles(times)

    def write_results(self, results, engine, namespace):
        """Write results to file."""
        u.write_results_to_file(results, engine, namespace)


class QueryGenerator(ABC):
    """Abstract base class for query generators."""

    def __init__(self, n_queries, k_neighbors, dataset):
        self.k_neighbors = k_neighbors

        if dataset == "deepimage96":
            dataset_obj = u.DatasetDeepImage96(max=n_queries)
            self.dataset = dataset_obj.test  # Use test vectors for queries
        elif dataset == "yfcc100m":
            dataset_obj = u.DatasetYFCC100M(max=n_queries)
            self.dataset = dataset_obj.test  # Use test vectors for queries

        if len(self.dataset) == 0:
            raise ValueError("Dataset vectors have zero length.")

        if len(self.dataset) < n_queries:
            raise ValueError(
                f"Requested {n_queries} queries, but dataset only has {len(self.dataset)} vectors.")

        self.dataset = self.dataset[:n_queries]  # Truncate to n_queries

        self.len = len(self.dataset)
        self.dim = len(self.dataset[0])

    def __len__(self):
        return self.len

    @abstractmethod
    def getitem(self, idx):
        """Get query item at index."""
        pass

    def __getitem__(self, idx):
        return self.getitem(idx)
