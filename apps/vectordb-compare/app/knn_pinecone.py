"""
Pinecone KNN Engine Implementation
"""

import os
import time
import threading
import numpy as np

from knn_base import KNNEngine, QueryGenerator, size_to_str
import utils as u


class QueryGeneratorPinecone(QueryGenerator):
    """Pinecone specific query generator."""

    def __init__(self, n_queries=int(1e6), k_neighbors=10, dataset="yfcc100m"):
        super().__init__(n_queries, k_neighbors, dataset)

    def getitem(self, idx):
        if idx < 0 or self.len <= idx:
            return None

        return self.dataset[idx].tolist()


def worker_pinecone(index, generator, namespace, knn_samples, start_index, end_index, times, results):
    """Worker function for Pinecone threading."""
    for i in range(start_index, end_index + 1):
        if i >= len(generator):
            break

        query_vector = generator[i]

        start_time = time.time()
        response = index.query(
            namespace=namespace,
            vector=query_vector,
            top_k=knn_samples,
            include_values=False,
        )

        assert (len(response["matches"]) == knn_samples)
        times[i] = time.time() - start_time

        for j in range(len(response["matches"])):
            results[i][j] = int(response["matches"][j]["id"].split("-")[1])


class PineconeEngine(KNNEngine):
    """Pinecone KNN benchmark engine."""

    def __init__(self):
        super().__init__("pinecone")

    def run_benchmark(self, params, grpc=False):
        """Run Pinecone KNN benchmark."""
        pc = u.create_connector_pinecone(grpc=grpc)
        index_name = f"knn-{params.dataset}"
        e_knn = self.setup_eval_tool(params)

        for s in params.sizes:
            namespace = f"{params.dataset}_{size_to_str(s)}"

            for c in params.concurrencies:
                print(f"Running knn for {namespace} on {c} threads...")

                generator = QueryGeneratorPinecone(params.total_queries, k_neighbors=params.knn_samples,
                                                   dataset=params.dataset)

                threads = []
                times = [0.0 for _ in range(len(generator))]
                results = [[0 for _ in range(params.knn_samples)]
                           for _ in range(len(generator))]

                th_queue_size = len(generator) // c

                index = pc.Index(index_name)
                for i in range(c):
                    start_index = i * len(generator) // c
                    end_index = min(
                        start_index + th_queue_size, len(generator))

                    t = threading.Thread(target=worker_pinecone, args=(
                        index, generator, namespace, params.knn_samples, start_index, end_index, times, results))
                    threads.append(t)
                    t.start()

                for t in threads:
                    t.join()

                engine = "pc_grpc" if grpc else "pc"

                knn_times = np.percentile(
                    np.array(times), params.stats_percentile)
                e_knn.add_row(f"knn_{params.knn_samples}", f"{engine}_perc_{params.stats_percentile}", s, c, params.total_queries,
                              knn_times, 0,  # ith percentile is a single value
                              params.knn_samples, 0)

                self.log_percentiles(times)
                self.write_results(results, engine, namespace)

        e_knn.export_to_csv()


def run_knn_pinecone(params, grpc=False):
    """Entry point for Pinecone KNN benchmark."""
    engine = PineconeEngine()
    engine.run_benchmark(params, grpc)
