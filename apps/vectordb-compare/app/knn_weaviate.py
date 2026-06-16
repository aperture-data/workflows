"""
Weaviate KNN Engine Implementation
"""

import os
import time
import threading
import numpy as np

from knn_base import KNNEngine, QueryGenerator, size_to_str
import utils as u


class QueryGeneratorWeaviate(QueryGenerator):
    """Weaviate specific query generator (same as Pinecone)."""

    def __init__(self, n_queries=int(1e6), k_neighbors=10, dataset="yfcc100m"):
        super().__init__(n_queries, k_neighbors, dataset)

    def getitem(self, idx):
        if idx < 0 or self.len <= idx:
            return None

        return self.dataset[idx].tolist()


def worker_weaviate(collection, generator, knn_samples, start_index, end_index, times, results):
    """Worker function for Weaviate threading."""
    for i in range(start_index, end_index + 1):
        if i >= len(generator):
            break

        query_vector = generator[i]

        start_time = time.time()
        response = collection.query.near_vector(
            near_vector=query_vector,
            limit=knn_samples,
        )
        assert (len(response.objects) == knn_samples)

        times[i] = time.time() - start_time

        for j in range(len(response.objects)):
            results[i][j] = response.objects[j].properties["test"]


class WeaviateEngine(KNNEngine):
    """Weaviate KNN benchmark engine."""

    def __init__(self):
        super().__init__("weaviate")

    def run_benchmark(self, params):
        """Run Weaviate KNN benchmark."""
        client = u.create_connector_weaviate()
        e_knn = self.setup_eval_tool(params)

        for s in params.sizes:
            collection_name = f"{params.dataset}_{size_to_str(s)}"
            collection = client.collections.get(collection_name)

            for c in params.concurrencies:
                print(f"Running knn for {collection_name} on {c} threads...")

                generator = QueryGeneratorWeaviate(params.total_queries, k_neighbors=params.knn_samples,
                                                   dataset=params.dataset)

                start_time = time.time()
                threads = []
                times = [0.0 for _ in range(len(generator))]
                results = [[0 for _ in range(params.knn_samples)]
                           for _ in range(len(generator))]

                th_queue_size = len(generator) // c
                for i in range(c):
                    start_index = i * len(generator) // c
                    end_index = min(
                        start_index + th_queue_size, len(generator))

                    t = threading.Thread(target=worker_weaviate, args=(
                        collection, generator, params.knn_samples, start_index, end_index, times, results))
                    threads.append(t)
                    t.start()

                for t in threads:
                    t.join()

                end_time = time.time()
                engine = "wv"  # Use consistent engine name matching command line

                knn_times = np.percentile(
                    np.array(times), params.stats_percentile)
                e_knn.add_row(f"knn_{params.knn_samples}", f"{engine}_perc_{params.stats_percentile}", s, c, params.total_queries,
                              knn_times, 0,  # ith percentile is a single value
                              params.knn_samples, 0)

                self.log_percentiles(times)
                self.write_results(results, engine, collection_name)

        e_knn.export_to_csv()
        client.close()


def run_knn_weaviate(params):
    """Entry point for Weaviate KNN benchmark."""
    engine = WeaviateEngine()
    engine.run_benchmark(params)
