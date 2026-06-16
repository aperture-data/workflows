"""
Main KNN Benchmark Runner with Dynamic Engine Loading
"""

import os
import argparse
import importlib
from dbeval import EvalTool
from config import create_config_from_args


def load_engine_module(engine_name):
    """
    Dynamically load engine module to avoid import issues when engine is not used.
    """
    engine_mapping = {
        'adb': 'knn_aperturedb',
        'adb_rest': 'knn_aperturedb',
        'pc': 'knn_pinecone',
        'pc_grpc': 'knn_pinecone',
        'wv': 'knn_weaviate',
        'qd': 'knn_qdrant',
        'ldb': 'knn_lancedb'
    }

    module_name = engine_mapping.get(engine_name)
    if not module_name:
        raise ValueError(f"Unknown engine: {engine_name}")

    try:
        return importlib.import_module(module_name)
    except ImportError as e:
        print(f"Failed to import engine {engine_name}: {e}")
        print(f"Skipping {engine_name} - required dependencies not available")
        return None


def main(params):
    """Main benchmark runner with dynamic engine loading."""

    if not params.keep_prev_output:
        eval_name = os.path.join(params.output_folder, "knn_comp")
        e_knn = EvalTool.EvalTool(eval_name)
        e_knn.clear()

    # Track which engines actually ran
    successful_engines = []

    try:
        if "adb" in params.engines:
            print("Running knn with ApertureDB with TCP...")
            module = load_engine_module("adb")
            if module:
                try:
                    module.run_knn_aperturedb(params, False)
                    successful_engines.append("adb")
                    print("✅ ApertureDB (TCP) completed successfully")
                except Exception as e:
                    print(f"❌ ApertureDB (TCP) failed: {e}")

        if "adb_rest" in params.engines:
            print("Running knn with ApertureDB with REST...")
            module = load_engine_module("adb_rest")
            if module:
                try:
                    module.run_knn_aperturedb(params, True)
                    # Keep separate for verification
                    successful_engines.append("adb_rest")
                    print("✅ ApertureDB (REST) completed successfully")
                except Exception as e:
                    print(f"❌ ApertureDB (REST) failed: {e}")

        if "pc" in params.engines:
            print("Running knn with Pinecone...")
            module = load_engine_module("pc")
            if module:
                try:
                    module.run_knn_pinecone(params, False)
                    successful_engines.append("pc")
                    print("✅ Pinecone completed successfully")
                except Exception as e:
                    print(f"❌ Pinecone failed: {e}")

        if "pc_grpc" in params.engines:
            print("Running knn with Pinecone with gRPC...")
            module = load_engine_module("pc_grpc")
            if module:
                try:
                    module.run_knn_pinecone(params, True)
                    successful_engines.append("pc_grpc")
                    print("✅ Pinecone (gRPC) completed successfully")
                except Exception as e:
                    print(f"❌ Pinecone (gRPC) failed: {e}")

        if "wv" in params.engines:
            print("Running knn with Weaviate...")
            module = load_engine_module("wv")
            if module:
                try:
                    module.run_knn_weaviate(params)
                    successful_engines.append("wv")
                    print("✅ Weaviate completed successfully")
                except Exception as e:
                    print(f"❌ Weaviate failed: {e}")

        if "qd" in params.engines:
            print("Running knn with Qdrant...")
            module = load_engine_module("qd")
            if module:
                try:
                    module.run_knn_qdrant(params)
                    successful_engines.append("qd")
                    print("✅ Qdrant completed successfully")
                except Exception as e:
                    print(f"❌ Qdrant failed: {e}")

        if "ldb" in params.engines:
            print("Running knn with LanceDB...")
            module = load_engine_module("ldb")
            if module:
                try:
                    module.run_lancedb_knn(params)
                    successful_engines.append("ldb")
                    print("✅ LanceDB completed successfully")
                except Exception as e:
                    print(f"❌ LanceDB failed: {e}")

    except Exception as e:
        print("Failed!")
        print(e)
        exit(1)


def get_args():
    """Parse command line arguments using centralized config."""
    return create_config_from_args("knn")


if __name__ == "__main__":
    args = get_args()

    print(f"Running benchmark with parameters: {args}")

    main(args)
