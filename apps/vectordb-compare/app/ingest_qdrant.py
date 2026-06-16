"""
Qdrant Ingestion Engine Implementation
"""

import time
import itertools

from ingest_base import IngestionEngine
import utils as u


class QdrantIngestionEngine(IngestionEngine):
    """Qdrant ingestion engine."""

    def __init__(self):
        super().__init__("qd")

    def chunks(self, iterable, batch_size):
        """A helper function to break an iterable into chunks of size batch_size."""
        it = iter(iterable)
        while True:
            chunk = list(itertools.islice(it, batch_size))
            if not chunk:
                break
            yield chunk

    def ingest_data(self, params):
        """Ingest data into Qdrant."""
        # Import qdrant modules only when needed
        from qdrant_client import models
        from qdrant_client.http.models import PointStruct

        client = u.create_connector_qdrant()

        self.log_progress(f"Qdrant collections: {client.get_collections()}")

        sizes = self.get_sizes_to_process(params.minimal)
        size_count = 1000

        for s in sizes:
            collection_name = f"{params.dataset}_{s}"

            self.log_progress(f"Starting load of namespace={s}...")

            # Load dataset
            self.log_progress(f"Loading {params.dataset} descriptors ...")
            dataset = self.get_dataset(params.dataset, size_count)
            self.log_progress(f"Done.")

            # Delete existing collection
            self.log_progress(f"Deleting existing vectors...")
            client.delete_collection(collection_name=collection_name)
            self.log_progress(f"Done.")

            # Create collection
            self.log_progress(f"Creating collection...")
            client.create_collection(
                collection_name=collection_name,
                vectors_config=models.VectorParams(
                    size=len(dataset[0]), distance=models.Distance.COSINE),
            )
            self.log_progress(f"Done.")

            self.log_progress(f"Adding {s} vectors ...")

            # Batch upsert vectors
            start_time = time.time()

            # Prepare points in batches
            points = [
                PointStruct(
                    id=i, vector=dataset[i].tolist(), payload={"id": i})
                for i in range(size_count)
            ]

            # Upload in chunks
            for chunk in self.chunks(points, params.load_batch_size):
                client.upsert(
                    collection_name=collection_name,
                    wait=True,
                    points=chunk
                )

            self.log_progress(
                f"Done {s}. Upsert elapsed time: {time.time() - start_time:.2f} s")
            size_count *= 10

        client.close()


def run_qdrant_ingestion(params):
    """Entry point for Qdrant ingestion."""
    engine = QdrantIngestionEngine()
    engine.ingest_data(params)
    return engine
