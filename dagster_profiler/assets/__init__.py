"""Example assets demonstrating profiling capabilities."""

from dagster_profiler.assets.profiled_assets import (
    cpu_intensive_computation,
    data_aggregation,
    data_transformation,
    generate_large_dataset,
    memory_heavy_processing,
    profile_results_summary,
)

__all__ = [
    "generate_large_dataset",
    "memory_heavy_processing",
    "cpu_intensive_computation",
    "data_transformation",
    "data_aggregation",
    "profile_results_summary",
]
