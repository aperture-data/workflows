"""
Pinecone Ingestion Engine Implementation
"""

import time
import itertools

from ingest_base import IngestionEngine
import utils as u


class PineconeIngestionEngine(IngestionEngine):
    """Pinecone ingestion engine."""

    def __init__(self):
        super().__init__("pc")

    def chunks(self, iterable, batch_size):
        """A helper function to break an iterable into chunks of size batch_size."""
        it = iter(iterable)
        while True:
            chunk = list(itertools.islice(it, batch_size))
            if not chunk:
                break
            yield chunk

    def ingest_data(self, params):
        """Ingest data into Pinecone."""
        pc = u.create_connector_pinecone(grpc=False)

        index_name = f"knn-{params.dataset}"
        index = pc.Index(index_name)

        sizes = self.get_sizes_to_process(params.minimal)
        size_count = 1000

        for s in sizes:
            namespace = f"{params.dataset}_{s}"
            self.log_progress(f"Starting load of namespace={namespace}...")

            # Load dataset
            self.log_progress(f"Loading {namespace} descriptors ...")
            dataset = self.get_dataset(params.dataset, size_count)
            self.log_progress(f"Dataset dimensions: {len(dataset[0])}")
            self.log_progress(f"Done.")

            # Prepare data generator
            data_generator = map(lambda i: (
                f'id-{i}', dataset[i]), range(size_count))

            self.log_progress(f"Deleting existing vectors ...")
            # Create a dummy vector to initialize namespace if it doesn't exist
            index.upsert(
                vectors=[
                    {
                        "id": f"id-{0}",
                        "values": dataset[0].tolist()
                    }
                ],
                async_req=False,
                namespace=namespace
            )

            # Delete all vectors in namespace
            index.delete(delete_all=True, namespace=namespace)
            self.log_progress(f"Done.")

            self.log_progress(f"Adding {s} vectors ...")

            # Batch upsert vectors
            start_time = time.time()

            for chunk in self.chunks(data_generator, params.load_batch_size):
                vectors_to_upsert = [
                    {
                        "id": vector_id,
                        "values": vector_data.tolist()
                    }
                    for vector_id, vector_data in chunk
                ]

                index.upsert(
                    vectors=vectors_to_upsert,
                    async_req=False,
                    namespace=namespace
                )

            duration = time.time() - start_time
            self.log_progress(
                f"Done {s}. Upsert elapsed time: {duration:.2f} s")

            # Record timing data for this size
            self.record_timing(s, duration)

            size_count *= 10


def run_pinecone_ingestion(params):
    """Entry point for Pinecone ingestion."""
    engine = PineconeIngestionEngine()
    engine.ingest_data(params)
    return engine
