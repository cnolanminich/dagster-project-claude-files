"""Job definitions that demonstrate different profiling scenarios."""

from dagster import define_asset_job, AssetSelection

# Job that focuses on memory-intensive assets
memory_profiling_job = define_asset_job(
    name="memory_profiling_job",
    description="Run memory-intensive assets with profiling",
    selection=AssetSelection.assets(
        "generate_large_dataset",
        "memory_heavy_processing",
    ),
)

# Job that focuses on CPU-intensive computation
cpu_profiling_job = define_asset_job(
    name="cpu_profiling_job",
    description="Run CPU-intensive assets with profiling",
    selection=AssetSelection.assets(
        "cpu_intensive_computation",
    ),
)

# Full pipeline job that exercises all profiled assets
full_profiling_job = define_asset_job(
    name="full_profiling_job",
    description="Run the complete profiling pipeline with all assets",
    selection=AssetSelection.assets(
        "generate_large_dataset",
        "memory_heavy_processing",
        "cpu_intensive_computation",
        "data_transformation",
        "data_aggregation",
        "profile_results_summary",
    ),
)
