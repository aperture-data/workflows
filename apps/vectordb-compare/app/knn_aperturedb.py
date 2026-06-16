"""
ApertureDB KNN Engine Implementation
"""

import os
import time
import threading
import numpy as np

from knn_base import KNNEngine, QueryGenerator, size_to_str

# ApertureDB imports - only imported when this module is used
from aperturedb import Utils, CommonLibrary
from aperturedb import Connector, ConnectorRest
from aperturedb import QueryGenerator as ADBQueryGenerator
from aperturedb import ParallelQuery


class QueryGeneratorApertureDB(QueryGenerator):
    """ApertureDB specific query generator."""

    def __init__(self, n_queries, set_name, k_neighbors, dataset):
        super().__init__(n_queries, k_neighbors, dataset)
        self.set_name = set_name

    def getitem(self, idx):
        if idx < 0 or self.len <= idx:
            return None

        descriptor = self.dataset[idx].astype('float32').tobytes()
        assert (len(descriptor) / 4 == self.dim)

        q = [{
            "FindDescriptor": {
                "set":         self.set_name,
                "k_neighbors": self.k_neighbors,
                "distances":   True,
                "results": {
                    "all_properties": True
                }
            }
        }]

        return q, [descriptor]

    def response_handler(self, q, b, r, r_b):
        assert (r[0]["FindDescriptor"]["returned"] == self.k_neighbors)


def worker_aperturedb(db, set_name, generator, knn_samples, start_index, end_index, times, results):
    """Worker function for ApertureDB threading."""
    local_db = CommonLibrary.create_connector()

    for i in range(start_index, end_index + 1):
        if i >= len(generator):
            break

        query, blobs = generator[i]
        start_time = time.time()
        response, _ = local_db.query(query, blobs)
        assert (response[0]["FindDescriptor"]["returned"] == knn_samples)

        times[i] = time.time() - start_time

        entities = response[0]["FindDescriptor"]["entities"]
        prop = "deep_img_id" if "deep" in set_name else "yfcc_id"
        for j in range(len(entities)):
            results[i][j] = entities[j][prop]


class ApertureDBEngine(KNNEngine):
    """ApertureDB KNN benchmark engine."""

    def __init__(self):
        super().__init__("aperturedb")

    def run_benchmark(self, params, use_rest=False):
        """Run ApertureDB KNN benchmark."""
        e_knn = self.setup_eval_tool(params)
        db = CommonLibrary.create_connector()

        # print all parameters
        print(f"Running ApertureDB KNN benchmark with parameters:")
        for key, value in params.__dict__.items():
            print(f"  {key}: {value}")

        for s in params.sizes:
            for c in params.concurrencies:
                print(f"Running knn for {size_to_str(s)} on {c} threads...")

                set_name = f"{params.dataset}_{size_to_str(s)}"

                generator = QueryGeneratorApertureDB(
                    params.total_queries, set_name=set_name,
                    k_neighbors=params.knn_samples, dataset=params.dataset)

                threads = []
                times = [0.0 for _ in range(len(generator))]
                results = [[0 for _ in range(params.knn_samples)]
                           for _ in range(len(generator))]

                th_queue_size = len(generator) // c
                for i in range(c):
                    start_index = i * len(generator) // c
                    end_index = min(
                        start_index + th_queue_size, len(generator))

                    t = threading.Thread(target=worker_aperturedb, args=(
                        db, set_name, generator, params.knn_samples, start_index, end_index, times, results))
                    threads.append(t)
                    t.start()

                for t in threads:
                    t.join()

                # Use different engine names for TCP vs REST to avoid file overwriting
                engine_name = "adb_rest" if use_rest else "adb"
                # Use detailed name for performance metrics
                engine_perf_name = "adb_th_rest" if use_rest else "adb_th"

                knn_times = np.percentile(
                    np.array(times), params.stats_percentile)
                e_knn.add_row(f"knn_{params.knn_samples}", f"{engine_perf_name}_perc_{params.stats_percentile}", s, c, params.total_queries,
                              knn_times, 0,  # ith percentile is a single value
                              params.knn_samples, 0)

                self.log_percentiles(times)
                # Use different engine names for file writing to avoid conflicts
                self.write_results(results, engine_name, set_name)

        e_knn.export_to_csv()


def run_knn_aperturedb(params, use_rest=False):
    """Entry point for ApertureDB KNN benchmark."""
    engine = ApertureDBEngine()
    engine.run_benchmark(params, use_rest)
