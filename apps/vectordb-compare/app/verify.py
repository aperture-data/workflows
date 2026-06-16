"""
Main Ingestion Verification Runner with Dynamic Engine Loading
"""

import argparse
import os
import importlib
import time
from config import create_config_from_args


def load_verification_engine(engine_name):
    """
    Dynamically load verification engine module to avoid import issues when engine is not used.
    """
    engine_mapping = {
        'adb': 'verify_aperturedb',
        'pc': 'verify_pinecone',
        'wv': 'verify_weaviate',
        'qd': 'verify_qdrant',
        'ldb': 'verify_lancedb'
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


def run_engine_verification(engine_name, source, size_str, dim, size_count):
    """Run verification for a specific engine and return timing info."""
    module = load_verification_engine(engine_name)
    if not module:
        return False, 0.0

    print(
        f"Verifying {engine_name} - {source}_{size_str} ({size_count:,} vectors)...")
    start_time = time.time()

    try:
        if engine_name == 'adb':
            result = module.verify_aperturedb(
                f"{source}_{size_str}", dim, size_count)
        elif engine_name == 'pc':
            result = module.verify_pinecone(
                source, f"{source}_{size_str}", dim, size_count)
        elif engine_name == 'wv':
            result = module.verify_weaviate(
                f"{source}_{size_str}", dim, size_count)
        elif engine_name == 'qd':
            result = module.verify_qdrant(
                f"{source}_{size_str}", dim, size_count)
        elif engine_name == 'ldb':
            result = module.verify_lancedb(
                f"{source}_{size_str}", dim, size_count)
        else:
            return False, 0.0

        elapsed_time = time.time() - start_time
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"  {status} - Query time: {elapsed_time:.3f}s")

        return result, elapsed_time

    except Exception as e:
        elapsed_time = time.time() - start_time
        print(f"  ❌ ERROR: {e} - Query time: {elapsed_time:.3f}s")
        return False, elapsed_time


def main(params):
    """Main verification runner with dynamic engine loading."""
    # Determine correct dimensions based on dataset
    if params.dataset == "deepimage96":
        params.dim = 96
    elif params.dataset == "yfcc100m":
        params.dim = 4096

    print(f"Verifying {params.dataset} dataset ({params.dim} dimensions)")
    print("=" * 80)

    sizes = ["1k", "10k", "100k", "1m"]
    all_good = True
    size_count = 1000

    # Track timing data for summary table
    timing_data = {}
    verification_results = {}

    for size_str in sizes:
        print(
            f"\n📊 Verifying dataset size: {size_str} ({size_count:,} vectors)")
        timing_data[size_str] = {}
        verification_results[size_str] = {}

        for engine in params.engines:
            result, elapsed_time = run_engine_verification(
                engine, params.dataset, size_str, params.dim, size_count)

            timing_data[size_str][engine] = elapsed_time
            verification_results[size_str][engine] = result
            all_good &= result

        size_count *= 10

    # Display verification timing summary table
    display_timing_summary(
        timing_data, verification_results, params.engines, sizes)

    if all_good:
        print(f"\n✅ Verification passed for {params.dataset}")
    else:
        print(f"\n❌ ERROR: Verification failed for {params.dataset}")

    return all_good


def display_timing_summary(timing_data, verification_results, engines, sizes):
    """Display a summary table of verification timing and results."""
    print("\n" + "=" * 80)
    print("VERIFICATION TIMING SUMMARY")
    print("=" * 80)

    # Header
    print(f"{'Dataset Size':<12}", end="")
    for engine in engines:
        print(f"{engine.upper():<12}", end="")
    print()

    print("-" * (12 + len(engines) * 12))

    # Data rows
    for size_str in sizes:
        print(f"{size_str:<12}", end="")
        for engine in engines:
            if engine in timing_data[size_str]:
                elapsed = timing_data[size_str][engine]
                success = verification_results[size_str][engine]
                status_symbol = "✅" if success else "❌"
                print(f"{elapsed:.3f}s {status_symbol:<6}", end="")
            else:
                print(f"{'N/A':<12}", end="")
        print()

    # Summary statistics
    print("\n📈 TIMING STATISTICS:")
    total_times = {}
    for engine in engines:
        engine_times = []
        for size_str in sizes:
            if engine in timing_data[size_str]:
                engine_times.append(timing_data[size_str][engine])

        if engine_times:
            total_time = sum(engine_times)
            avg_time = total_time / len(engine_times)
            max_time = max(engine_times)
            min_time = min(engine_times)

            print(f"  {engine.upper():<5}: Total: {total_time:.3f}s | Avg: {avg_time:.3f}s | Min: {min_time:.3f}s | Max: {max_time:.3f}s")
            total_times[engine] = total_time

    # Fastest/slowest engines
    if total_times:
        fastest_engine = min(total_times.keys(), key=lambda x: total_times[x])
        slowest_engine = max(total_times.keys(), key=lambda x: total_times[x])
        print(
            f"\n🏃 Fastest Engine: {fastest_engine.upper()} ({total_times[fastest_engine]:.3f}s total)")
        print(
            f"🐢 Slowest Engine: {slowest_engine.upper()} ({total_times[slowest_engine]:.3f}s total)")


def get_args():
    """Parse command line arguments using centralized config."""
    return create_config_from_args("verify")


if __name__ == "__main__":
    args = get_args()
    main(args)
