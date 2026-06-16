"""
Weaviate Ingestion Engine Implementation
"""

import time
import itertools

from ingest_base import IngestionEngine
import utils as u


class WeaviateIngestionEngine(IngestionEngine):
    """Weaviate ingestion engine."""

    def __init__(self):
        super().__init__("wv")

    def ingest_data(self, params):
        """Ingest data into Weaviate."""
        # Import weaviate modules only when needed
        import weaviate
        from weaviate.classes.config import Configure, Property, DataType
        from weaviate.classes.data import DataObject

        sizes = self.get_sizes_to_process(params.minimal)

        for s in sizes:
            size_count = params.str_size_to_int(s)
            client = u.create_connector_weaviate()
            collection_name = f"{params.dataset}_{s}"

            try:
                self.log_progress(f"Starting load of namespace={s}...")

                # Load dataset
                self.log_progress(f"Loading {params.dataset} descriptors ...")
                dataset = self.get_dataset(params.dataset, size_count)
                self.log_progress(f"Done.")

                # Delete existing collection
                self.log_progress(f"Deleting existing vectors...")
                client.collections.delete(collection_name)
                self.log_progress(f"Done.")

                # Create collection
                client.collections.create(
                    collection_name,
                    properties=[
                        Property(name="test", data_type=DataType.INT),
                    ],
                    vector_config=Configure.Vectors.self_provided(
                        vector_index_config=Configure.VectorIndex.hnsw()
                    )
                )

                collection = client.collections.get(collection_name)

                self.log_progress(f"Adding {s} vectors ...")

                # Batch insert vectors
                start_time = time.time()

                # Prepare data in batches
                batch_size = params.load_batch_size
                for i in range(0, size_count, batch_size):
                    end_idx = min(i + batch_size, size_count)
                    batch_objects = []

                    for idx in range(i, end_idx):
                        batch_objects.append(DataObject(
                            properties={"test": idx},
                            vector=dataset[idx].tolist()
                        ))

                    collection.data.insert_many(batch_objects)

                self.log_progress(
                    f"Done {s}. Upsert elapsed time: {time.time() - start_time:.2f} s")

            finally:
                client.close()


def run_weaviate_ingestion(params):
    """Entry point for Weaviate ingestion."""
    engine = WeaviateIngestionEngine()
    engine.ingest_data(params)
    return engine
