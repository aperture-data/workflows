"""
LanceDB Ingestion Engine Implementation
"""

import time
import os
import tempfile
import shutil

from ingest_base import IngestionEngine
import utils as u


class LanceDBIngestionEngine(IngestionEngine):
    """LanceDB ingestion engine."""

    def __init__(self):
        super().__init__("ldb")

    def ingest_data(self, params):
        """Ingest data into LanceDB."""
        import pandas as pd

        # Use centralized connector from utils
        db = u.create_connector_lancedb()

        self.log_progress(f"Connected to LanceDB Cloud")

        sizes = self.get_sizes_to_process(params.minimal)
        size_count = 1000

        for s in sizes:
            table_name = f"{params.dataset}_{s}"
            self.log_progress(f"Starting load of table={table_name}...")

            # Load dataset
            self.log_progress(f"Loading {size_count} descriptors ...")
            dataset = self.get_dataset(params.dataset, size_count)
            self.log_progress(f"Dataset dimensions: {len(dataset[0])}")
            self.log_progress(f"Done.")

            # Drop existing table if it exists
            try:
                if table_name in db.table_names():
                    db.drop_table(table_name)
                    self.log_progress(f"Dropped existing table: {table_name}")
            except Exception as e:
                self.log_progress(
                    f"Warning: Could not drop table {table_name}: {e}")

            # Prepare data for ingestion
            self.log_progress(f"Preparing data for ingestion...")

            # Create table and ingest data in batches
            self.log_progress(f"Adding {size_count} vectors...")
            start_time = time.time()

            # Create list of records with vector and metadata
            records = []
            for i in range(size_count):
                vector = dataset[i].astype('float32').tolist()
                record = {
                    'id': i,
                    'vector': vector
                }

                # Add source-specific metadata
                if params.dataset == "deepimage96":
                    record['deep_img_id'] = i
                elif params.dataset == "yfcc100m":
                    record['yfcc_id'] = i

                records.append(record)

            # Process in batches to avoid memory issues
            batch_size = min(params.load_batch_size, 1000)

            # Create table with first batch
            first_batch = records[:batch_size]
            df = pd.DataFrame(first_batch)
            table = db.create_table(table_name, df)

            # Add remaining batches
            for i in range(batch_size, len(records), batch_size):
                batch = records[i:i + batch_size]
                batch_df = pd.DataFrame(batch)
                table.add(batch_df)

            # LanceDB automatically creates optimal indexes internally
            # Skip explicit index creation as it may cause API compatibility issues
            self.log_progress(
                f"Using LanceDB's automatic indexing (HNSW-based)")

            # Note: LanceDB automatically creates efficient indexes for vector columns
            # Manual index creation is often not needed and can cause compatibility issues
            # with different LanceDB versions

            elapsed_time = time.time() - start_time
            self.log_progress(
                f"Done {s}. Ingestion elapsed time: {elapsed_time:.2f} s")

            size_count *= 10


def run_lancedb_ingestion(params):
    """Entry point for LanceDB ingestion."""
    engine = LanceDBIngestionEngine()
    engine.ingest_data(params)
    return engine
