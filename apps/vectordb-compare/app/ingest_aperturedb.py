"""
ApertureDB Ingestion Engine Implementation
"""

import time
import numpy as np

from ingest_base import IngestionEngine

# ApertureDB imports - only imported when this module is used
from aperturedb import Utils, CommonLibrary
from aperturedb import QueryGenerator
from aperturedb import ParallelLoader


class DeepImage96Generator(QueryGenerator.QueryGenerator):
    """Deep Image 96 dataset generator for ApertureDB."""

    def __init__(self, dataset, set_name="yfcc_descriptors"):
        self.set_name = set_name
        self.dataset = dataset
        self.len = len(self.dataset)
        self.dim = len(self.dataset[0])

    def __len__(self):
        return self.len

    def getitem(self, idx):
        if idx < 0 or self.len <= idx:
            return None

        descriptor = self.dataset[idx].astype('float32').tobytes()
        assert (len(descriptor) / 4 == self.dim)

        q = [{
            "AddDescriptor": {
                "set": self.set_name,
                "properties": {
                    "deep_img_id": idx,
                },
            }
        }]

        return q, [descriptor]


class YFCC100MDescriptorsGenerator(QueryGenerator.QueryGenerator):
    """YFCC100M dataset generator for ApertureDB."""

    def __init__(self, dataset, set_name="yfcc_descriptors"):
        self.set_name = set_name
        self.dataset = dataset
        self.len = len(self.dataset)
        self.dim = len(self.dataset[0])

    def __len__(self):
        return self.len

    def getitem(self, idx):
        if idx < 0 or self.len <= idx:
            return None

        descriptor = self.dataset[idx].astype('float32').tobytes()
        assert (len(descriptor) / 4 == self.dim)

        q = [{
            "AddDescriptor": {
                "set": self.set_name,
                "properties": {
                    "yfcc_id": idx,
                },
            }
        }]

        return q, [descriptor]


class ApertureDBIngestionEngine(IngestionEngine):
    """ApertureDB ingestion engine."""

    def __init__(self):
        super().__init__("adb")

    def ingest_data(self, params):
        """Ingest data into ApertureDB."""
        db = CommonLibrary.create_connector()
        dbutils = Utils.Utils(db)

        self.log_progress(f"ApertureDB status: {dbutils.status()}")

        # Create important indexes for performance
        dbutils.create_entity_index("_Descriptor", "_create_txn")
        dbutils.create_entity_index("_DescriptorSet", "_name")

        sizes = self.get_sizes_to_process(params.minimal)

        for s in sizes:
            set_name = f"{params.dataset}_{s}"

            self.log_progress(f"Starting load of set={set_name}...")

            # Get dataset
            dataset = self.get_dataset(params.dataset, self.size_to_count(s))

            # Create generator
            if params.dataset == "deepimage96":
                generator = DeepImage96Generator(dataset, set_name=set_name)
            elif params.dataset == "yfcc100m":
                generator = YFCC100MDescriptorsGenerator(
                    dataset, set_name=set_name)

            self.log_progress(f"Dataset loaded.")

            # Remove existing data
            self.log_progress(f"Deleting existing vectors ...")
            dbutils.remove_descriptorset(set_name)
            self.log_progress(f"Done.")

            # Add descriptor set
            dbutils.add_descriptorset(set_name, generator.dim, "CS", "HNSW")

            self.log_progress(f"Adding {s} vectors ...")

            # Ingest data
            start_time = time.time()
            loader = ParallelLoader.ParallelLoader(db)
            loader.ingest(generator,
                          batchsize=params.load_batch_size,
                          numthreads=params.load_num_threads,
                          stats=params.verbose)

            duration = time.time() - start_time
            self.log_progress(
                f"Done {s}. Upsert elapsed time: {duration:.2f} s")

            # Record timing data for this size
            self.record_timing(s, duration)


def run_aperturedb_ingestion(params):
    """Entry point for ApertureDB ingestion."""
    engine = ApertureDBIngestionEngine()
    engine.ingest_data(params)
    return engine
