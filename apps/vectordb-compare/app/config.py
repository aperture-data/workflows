"""
Shared Configuration Class for Vector Database Benchmark
Centralizes environment variable handling and parameter management
"""

import os
import argparse
from typing import List, Union, Optional


class BenchmarkConfig:
    """
    Centralized configuration class for vector database benchmarking.
    Handles environment variables and command line arguments.
    """

    def __init__(self):
        # Core dataset parameters
        self.dataset = os.environ.get('DATASET', "deepimage96")
        self.dim = self._get_dataset_dimensions()
        self.minimal = self._str_to_bool(os.environ.get('MINIMAL', False))

        # Ingestion parameters
        self.load_batch_size = int(os.environ.get('LOAD_BATCHSIZE', 1000))
        self.load_num_threads = int(os.environ.get('LOAD_NUMTHREADS', 32))

        # KNN/Query parameters
        self.knn_samples = int(os.environ.get('KNN_SAMPLES', 10))
        self.total_queries = int(os.environ.get('TOTAL_QUERIES', 100))
        self.stats_percentile = int(os.environ.get('STATS_PERCENTILE', 90))

        # Engine and size parameters
        self.engines = self._parse_list(
            os.environ.get('ENGINES', "adb,pc,wv,qd,ldb"))
        self.sizes = self._parse_int_list(
            os.environ.get('SIZES', "1000,10000,100000,1000000"))
        self.concurrencies = self._parse_int_list(
            os.environ.get('CONCURRENCIES', "8,16,32,64"))

        # Output and runtime parameters
        self.keep_prev_output = self._str_to_bool(
            os.environ.get('KEEP_PREV_OUTPUT', False))
        self.delete_existing = self._str_to_bool(
            os.environ.get('DELETE_EXISTING', True))
        self.run_name = os.environ.get('RUN_NAME', "")
        self.output_folder = "output"
        self.verbose = True

        # Metrics (for ingestion)
        self.metrics = self._parse_list(os.environ.get('METRICS', "CS"))

    def _get_dataset_dimensions(self) -> int:
        """Get the correct dimensions based on dataset type."""
        if self.dataset == "deepimage96":
            return int(os.environ.get('DIM', 96))
        elif self.dataset == "yfcc100m":
            return int(os.environ.get('DIM', 4096))
        else:
            return int(os.environ.get('DIM', 64))

    def _str_to_bool(self, value: Union[str, bool]) -> bool:
        """Convert string to boolean."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ('yes', 'true', 't', 'y', '1')
        return False

    def _parse_list(self, value: str) -> List[str]:
        """Parse comma-separated string into list of strings."""
        if not value:
            return []
        return [item.strip() for item in value.split(',')]

    def _parse_int_list(self, value: str) -> List[int]:
        """Parse comma-separated string into list of integers."""
        if not value:
            return []
        return [int(item.strip()) for item in value.split(',')]

    def str_size_to_int(self, size_str: str) -> int:
        """Convert size string (e.g., '1k', '10k') to integer."""
        size_mapping = {
            'k': 1000,
            'm': 1000000
        }
        if size_str[-1] in size_mapping:
            return int(size_str[:-1]) * size_mapping[size_str[-1]]
        return int(size_str)

    def get_sizes_to_process(self, minimal: Optional[bool] = None) -> List[str]:
        """
        Get list of size strings to process based on minimal flag.
        Returns size strings like ['1k', '10k', '100k', '1m']
        """
        if minimal is None:
            minimal = self.minimal

        size_mapping = {
            1000: "1k",
            10000: "10k",
            100000: "100k",
            1000000: "1m"
        }

        if minimal:
            return ["1k"]
        else:
            return [size_mapping.get(size, str(size)) for size in self.sizes]

    def validate_dataset(self):
        """Validate that the dataset is supported."""
        supported_datasets = ["yfcc100m", "deepimage96"]
        if self.dataset not in supported_datasets:
            raise ValueError(
                f"Unknown dataset: {self.dataset}. Supported: {supported_datasets}")

    def update_from_args(self, args: argparse.Namespace):
        """Update configuration from command line arguments."""
        for key, value in vars(args).items():
            if hasattr(self, key) and value is not None:
                setattr(self, key, value)

    def create_argparser(self, parser_type: str = "common") -> argparse.ArgumentParser:
        """
        Create argument parser with appropriate arguments for different script types.

        Args:
            parser_type: One of 'ingest', 'verify', 'knn', or 'common'
        """
        parser = argparse.ArgumentParser()

        # Common arguments for all scripts
        parser.add_argument('-minimal', type=self._str_to_bool,
                            default=self.minimal,
                            help='Run in minimal mode (1k records only)')

        parser.add_argument('-dataset', type=str,
                            default=self.dataset,
                            help='Dataset to use (deepimage96, yfcc100m)')

        parser.add_argument('-engines', type=str,
                            default=','.join(self.engines),
                            help='Comma-separated list of engines to test')

        parser.add_argument('-output_folder', type=str,
                            default=self.output_folder,
                            help='Output folder for results')

        parser.add_argument('-verbose', type=self._str_to_bool,
                            default=self.verbose,
                            help='Enable verbose output')

        # Script-specific arguments
        if parser_type == "ingest":
            parser.add_argument('-load_batch_size', type=int,
                                default=self.load_batch_size,
                                help='Batch size for loading data')

            parser.add_argument('-load_num_threads', type=int,
                                default=self.load_num_threads,
                                help='Number of threads for loading')

            parser.add_argument('-metrics', type=str,
                                default=','.join(self.metrics),
                                help='Metrics to use')

        elif parser_type == "knn":
            parser.add_argument('-knn_samples', type=int,
                                default=self.knn_samples,
                                help='Number of KNN samples')

            parser.add_argument('-total_queries', type=int,
                                default=self.total_queries,
                                help='Total number of queries to run')

            parser.add_argument('-stats_percentile', type=int,
                                default=self.stats_percentile,
                                help='Percentile for statistics')

            parser.add_argument('-concurrencies', type=str,
                                default=','.join(map(str, self.concurrencies)),
                                help='Comma-separated list of concurrency levels')

            parser.add_argument('-sizes', type=str,
                                default=','.join(map(str, self.sizes)),
                                help='Comma-separated list of dataset sizes')

        elif parser_type == "verify":
            parser.add_argument('-delete_existing', type=self._str_to_bool,
                                default=self.delete_existing,
                                help='Delete existing data before verification')

        # Common optional arguments
        parser.add_argument('-keep_prev_output', type=self._str_to_bool,
                            default=self.keep_prev_output,
                            help='Keep previous output files')

        parser.add_argument('-run_name', type=str,
                            default=self.run_name,
                            help='Name for this benchmark run')

        return parser

    def __str__(self) -> str:
        """String representation of configuration."""
        return (f"BenchmarkConfig("
                f"dataset={self.dataset}, "
                f"dim={self.dim}, "
                f"engines={self.engines}, "
                f"minimal={self.minimal})")

    def __repr__(self) -> str:
        return self.__str__()


# Global configuration instance
config = BenchmarkConfig()


def get_config() -> BenchmarkConfig:
    """Get the global configuration instance."""
    return config


def create_config_from_args(parser_type: str = "common",
                            custom_args: Optional[List[str]] = None) -> BenchmarkConfig:
    """
    Create and return a configuration instance from command line arguments.

    Args:
        parser_type: Type of parser to create ('ingest', 'verify', 'knn', 'common')
        custom_args: Custom arguments list (for testing), uses sys.argv if None

    Returns:
        BenchmarkConfig instance configured from command line arguments
    """
    config = BenchmarkConfig()
    parser = config.create_argparser(parser_type)

    if custom_args is not None:
        args = parser.parse_args(custom_args)
    else:
        args = parser.parse_args()

    # Update config with parsed arguments
    config.update_from_args(args)

    # Parse list arguments
    if hasattr(args, 'engines'):
        config.engines = config._parse_list(args.engines)
    if hasattr(args, 'metrics'):
        config.metrics = config._parse_list(args.metrics)
    if hasattr(args, 'concurrencies'):
        config.concurrencies = config._parse_int_list(args.concurrencies)
    if hasattr(args, 'sizes'):
        config.sizes = config._parse_int_list(args.sizes)

    # Validate configuration
    config.validate_dataset()

    return config
