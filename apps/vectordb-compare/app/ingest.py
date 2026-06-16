"""
Main Ingestion Runner with Dynamic Engine Loading
"""

import os
import argparse
import importlib
import time
from collections import defaultdict
from config import create_config_from_args


def load_ingestion_engine(engine_name):
    """
    Dynamically load ingestion engine module to avoid import issues when engine is not used.
    """
    engine_mapping = {
        'adb': 'ingest_aperturedb',
        'pc': 'ingest_pinecone',
        'wv': 'ingest_weaviate',
        'qd': 'ingest_qdrant',
        'ldb': 'ingest_lancedb'
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


def run_engine_ingestion(engine_name, params, timing_data):
    """Run ingestion for a specific engine and collect timing data."""
    module = load_ingestion_engine(engine_name)
    if not module:
        return False

    # Record start time
    engine_start_time = time.time()

    engine_instance = None
    if engine_name == 'adb':
        engine_instance = module.run_aperturedb_ingestion(params)
    elif engine_name == 'pc':
        engine_instance = module.run_pinecone_ingestion(params)
    elif engine_name == 'wv':
        engine_instance = module.run_weaviate_ingestion(params)
    elif engine_name == 'qd':
        engine_instance = module.run_qdrant_ingestion(params)
    elif engine_name == 'ldb':
        engine_instance = module.run_lancedb_ingestion(params)

    # Record end time and calculate duration
    engine_end_time = time.time()
    total_duration = engine_end_time - engine_start_time

    # Get detailed timing data from the engine if available
    if engine_instance and hasattr(engine_instance, 'get_timing_data'):
        detailed_timing = engine_instance.get_timing_data()
        timing_data[engine_name.upper()].update(detailed_timing)

    # Always record total time
    timing_data[engine_name.upper()]['total'] = total_duration

    return True


def print_timing_summary(timing_data, params):
    """Print a summary table of ingestion times."""
    print("\n" + "=" * 80)
    print("INGESTION TIMING SUMMARY")
    print("=" * 80)

    # Get dataset sizes
    sizes = ["1k"] if params.minimal else ["1k", "10k", "100k", "1m"]

    # Print table header
    header = f"{'Engine':<12}"
    for size in sizes:
        header += f"{size:>12}"
    header += f"{'Total (s)':>12}"
    print(header)
    print("-" * len(header))

    # Print data for each engine
    for engine, data in timing_data.items():
        if data:  # Only print engines that actually ran
            row = f"{engine:<12}"

            # Check if we have detailed per-size timing data
            has_detailed_timing = any(size in data for size in sizes)

            if has_detailed_timing:
                # Use actual per-size timing data
                for size in sizes:
                    if size in data:
                        row += f"{data[size]:>11.1f}s"
                    else:
                        row += f"{'N/A':>12}"
            else:
                # Fall back to distributing total time evenly
                if 'total' in data:
                    avg_time = data['total'] / len(sizes)
                    for size in sizes:
                        row += f"{avg_time:>11.1f}s"
                else:
                    for size in sizes:
                        row += f"{'N/A':>12}"

            # Add total time
            if 'total' in data:
                row += f"{data['total']:>11.1f}s"
            else:
                row += f"{'N/A':>12}"

            print(row)

    print("-" * len(header))
    print(f"\nDataset: {params.dataset}")
    print(f"Dimensions: {params.dim}")
    print(f"Batch size: {params.load_batch_size}")
    print(f"Threads: {params.load_num_threads}")
    if params.minimal:
        print("Mode: Minimal (1k records only)")
    else:
        print("Mode: Full (1k, 10k, 100k, 1m records)")
    print("=" * 80)


def main(params):
    """Main ingestion runner with dynamic engine loading."""
    # Initialize timing data collection
    timing_data = defaultdict(dict)

    try:
        for engine in params.engines:
            print(f"Running ingestion with {engine.upper()}...")
            success = run_engine_ingestion(engine, params, timing_data)
            if success:
                print(f"✓ {engine.upper()} ingestion completed")
            else:
                print(
                    f"✗ {engine.upper()} ingestion skipped due to missing dependencies")
            print("-" * 50)

        # Print timing summary after all engines complete
        if timing_data:
            print_timing_summary(timing_data, params)

    except Exception as e:
        print("Failed!")
        print(e)
        exit(1)


def get_args():
    """Parse command line arguments using centralized config."""
    return create_config_from_args("ingest")


if __name__ == "__main__":
    args = get_args()
    main(args)
