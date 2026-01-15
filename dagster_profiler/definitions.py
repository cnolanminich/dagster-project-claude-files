"""Dagster definitions for the performance profiler project."""

import os

from dagster import Definitions

from dagster_profiler.assets import (
    cpu_intensive_computation,
    data_aggregation,
    data_transformation,
    generate_large_dataset,
    memory_heavy_processing,
    profile_results_summary,
)
from dagster_profiler.assets.profiled_assets import parallel_profiled_tasks
from dagster_profiler.jobs import (
    cpu_profiling_job,
    full_profiling_job,
    memory_profiling_job,
)
from dagster_profiler.resources import ProfilingResource

# Configure the profiling resource
# Output directory can be configured via environment variable
PROFILE_OUTPUT_DIR = os.environ.get("DAGSTER_PROFILE_OUTPUT_DIR", "/tmp/dagster_profiles")

defs = Definitions(
    assets=[
        generate_large_dataset,
        memory_heavy_processing,
        cpu_intensive_computation,
        data_transformation,
        data_aggregation,
        profile_results_summary,
        parallel_profiled_tasks,
    ],
    jobs=[
        memory_profiling_job,
        cpu_profiling_job,
        full_profiling_job,
    ],
    resources={
        "profiler": ProfilingResource(
            output_dir=PROFILE_OUTPUT_DIR,
            enable_memray=True,
            enable_cprofile=True,
            track_native_memory=False,
            cpu_monitor_interval=0.1,
        ),
    },
)
