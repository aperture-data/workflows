"""
LanceDB KNN Engine Implementation
"""

import os
import time
import threading
import numpy as np

from knn_base import KNNEngine, QueryGenerator, size_to_str
import utils as u
from dbeval import EvalTool


class QueryGeneratorLanceDB(QueryGenerator):
    """LanceDB specific query generator."""

    def __init__(self, n_queries=int(1e6), table_name="yfcc_descriptors", k_neighbors=10,
                 dataset="yfcc100m", db=None):
        super().__init__(n_queries, k_neighbors, dataset)
        self.table_name = table_name
        self.db = db
        self.table = None

        # Get the table reference
        if self.db and table_name in self.db.table_names():
            self.table = self.db.open_table(table_name)

    def getitem(self, idx):
        if idx < 0 or self.len <= idx:
            return None

        # Get query vector
        query_vector = self.dataset[idx].astype('float32').tolist()
        return query_vector, None

    def search(self, query_vector):
        """Perform KNN search using LanceDB."""
        if not self.table:
            raise ValueError(f"Table {self.table_name} not available")

        # Perform vector search
        results = self.table.search(query_vector).limit(
            self.k_neighbors).to_pandas()
        return results

    def response_handler(self, query_vector, blob_data, results, result_blobs):
        """Handle search results."""
        if len(results) != self.k_neighbors:
            raise ValueError(
                f"Expected {self.k_neighbors} results, got {len(results)}")


class LanceDBKNNEngine(KNNEngine):
    """LanceDB KNN benchmark engine."""

    def __init__(self):
        super().__init__("ldb")

    def run_benchmark(self, params):
        """Run KNN benchmark for LanceDB."""
        import utils as u

        # Use centralized connector from utils
        db = u.create_connector_lancedb()

        print(f"Connected to LanceDB Cloud")
        print(f"Available tables: {db.table_names()}")

        eval_tool = self.setup_eval_tool(params)
        # Use the sizes from parameters, not hardcoded values
        sizes = params.sizes

        for s in sizes:
            table_name = f"{params.dataset}_{size_to_str(s)}"

            if table_name not in db.table_names():
                print(f"Table {table_name} not found, skipping...")
                continue

            print(f"Running benchmark for size: {size_to_str(s)}")

            # Load query dataset - will now use TEST vectors via updated base class
            if params.dataset == "deepimage96":
                query_data = u.DatasetDeepImage96(
                    max=params.total_queries).test
            elif params.dataset == "yfcc100m":
                query_data = u.DatasetYFCC100M(max=params.total_queries).test
            else:
                raise ValueError(f"Unknown dataset: {params.dataset}")

            print(f"Using {len(query_data)} test vectors as queries")

            # Create query generator
            qg = QueryGeneratorLanceDB(
                n_queries=params.total_queries,
                table_name=table_name,
                k_neighbors=params.knn_samples,
                dataset=params.dataset,
                db=db
            )
            qg.dataset = query_data

            # Collect KNN results for verification (only once per size)
            print(f"Collecting KNN results for verification...")
            verification_results = []
            # Limit to avoid too many results
            for query_idx in range(min(params.total_queries, 100)):
                query_vector, _ = qg.getitem(query_idx)
                results = qg.search(query_vector)

                # Debug: Print the structure of the first result
                if query_idx == 0:
                    print(f"LanceDB result structure:")
                    print(f"  Type: {type(results)}")
                    print(
                        f"  Shape: {results.shape if hasattr(results, 'shape') else 'N/A'}")
                    print(
                        f"  Columns: {list(results.columns) if hasattr(results, 'columns') else 'N/A'}")
                    if len(results) > 0:
                        print(f"  Sample data: {results.head(2)}")

                # Extract neighbor IDs from results - LanceDB typically uses row index or a specific ID column
                neighbor_ids = []
                try:
                    if hasattr(results, 'columns') and len(results) > 0:
                        # Check for original dataset ID column first (for ground truth comparison)
                        if 'deep_img_id' in results.columns:
                            neighbor_ids = results['deep_img_id'].tolist()[
                                :params.knn_samples]
                        elif 'id' in results.columns:
                            neighbor_ids = results['id'].tolist()[
                                :params.knn_samples]
                        elif '_rowid' in results.columns:
                            neighbor_ids = results['_rowid'].tolist()[
                                :params.knn_samples]
                        elif len(results.columns) > 0:
                            # Use the first column as ID (common in vector DBs)
                            first_col = results.columns[0]
                            neighbor_ids = results[first_col].tolist()[
                                :params.knn_samples]
                        else:
                            # Use DataFrame index as IDs
                            neighbor_ids = results.index.tolist()[
                                :params.knn_samples]
                    elif hasattr(results, 'to_list'):
                        # Alternative format
                        neighbor_ids = results.to_list()[:params.knn_samples]
                except Exception as e:
                    print(
                        f"Error extracting neighbor IDs for query {query_idx}: {e}")
                    # Fill with zeros as fallback
                    neighbor_ids = [0] * params.knn_samples

                # Ensure we have the right number of neighbors
                while len(neighbor_ids) < params.knn_samples:
                    neighbor_ids.append(0)  # Pad with zeros if needed

                # Debug: Print first few results to understand ID mapping
                if query_idx < 3:  # Debug first 3 queries
                    print(
                        f"    Query {query_idx}: Engine returned {neighbor_ids[:5]}")

                verification_results.append(neighbor_ids[:params.knn_samples])

            # Write results to file for verification
            if verification_results:
                print(
                    f"Writing {len(verification_results)} results to file...")
                set_name = f"{params.dataset}_{size_to_str(s)}"
                self.write_results(verification_results, "ldb", set_name)

            # Test different concurrency levels
            for concurrency in params.concurrencies:
                print(f"Testing concurrency: {concurrency}")

                def worker_function(worker_id):
                    """Worker function for concurrent queries."""
                    times = []

                    queries_per_worker = params.total_queries // concurrency
                    start_idx = worker_id * queries_per_worker

                    for i in range(queries_per_worker):
                        query_idx = start_idx + i
                        if query_idx >= params.total_queries:
                            break

                        query_vector, _ = qg.getitem(query_idx)

                        start_time = time.time()
                        results = qg.search(query_vector)
                        elapsed_time = time.time() - start_time

                        times.append(elapsed_time)

                        # Handle response for validation
                        qg.response_handler(query_vector, None, results, None)

                    return times

                # Run concurrent queries
                threads = []
                all_times = []

                start_total = time.time()

                for worker_id in range(concurrency):
                    thread = threading.Thread(
                        target=lambda wid=worker_id: all_times.extend(worker_function(wid)))
                    threads.append(thread)
                    thread.start()

                for thread in threads:
                    thread.join()

                total_time = time.time() - start_total
                avg_time = np.mean(all_times) if all_times else 0
                throughput = len(all_times) / \
                    total_time if total_time > 0 else 0

                print(f"Concurrency {concurrency}: "
                      f"Avg time: {avg_time:.4f}s, "
                      f"Throughput: {throughput:.2f} QPS")

                # Log percentiles
                if all_times:
                    self.log_percentiles(all_times)

                # Record results using correct EvalTool API
                knn_times = np.percentile(
                    all_times, params.stats_percentile) if all_times else 0
                eval_tool.add_row(
                    f"knn_{params.knn_samples}",
                    f"ldb_perc_{params.stats_percentile}",
                    s,  # size
                    concurrency,  # concurrency
                    params.total_queries,
                    knn_times,  # percentile time
                    0,  # ith percentile is a single value
                    params.knn_samples,
                    0
                )

        eval_tool.export_to_csv()
        print("LanceDB benchmark completed!")


def run_lancedb_knn(params):
    """Entry point for LanceDB KNN benchmark."""
    engine = LanceDBKNNEngine()
    engine.run_benchmark(params)
