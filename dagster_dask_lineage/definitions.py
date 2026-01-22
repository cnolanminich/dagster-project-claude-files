"""Dagster definitions for the dagster-dask-lineage project.

This module configures the Dagster code location with:
- Dask resource for parallel DataFrame processing WITHIN assets
- Jobs using dagster-dask's dask_executor for distributed STEP execution
- Assets demonstrating schema and lineage tracking

KEY INSIGHT: You can use BOTH approaches together!
==================================================
1. `dask_executor` (from dagster-dask): Distributes assets across Dask workers
2. Dask DataFrames within assets: Parallel processing + schema extraction

The `combined_dask_job` demonstrates both working together.
"""

from dagster import Definitions, load_assets_from_modules

from dagster_dask_lineage import assets, assets_combined
from dagster_dask_lineage.jobs import (
    combined_dask_job,
    dask_distributed_job,
    default_executor_job,
)
from dagster_dask_lineage.resources import DaskResource

# Load all assets from both modules
all_assets = load_assets_from_modules([assets, assets_combined])

# Configure resources
# Note: This DaskResource is our CUSTOM resource for in-asset parallelism
# It is NOT from the dagster-dask library
# The combined_example assets don't need this resource - they create
# Dask DataFrames directly without a managed client
resources = {
    "dask": DaskResource(
        cluster_type="local",
        n_workers=4,
        threads_per_worker=2,
        memory_limit="2GB",
    ),
}

defs = Definitions(
    assets=all_assets,
    jobs=[dask_distributed_job, default_executor_job, combined_dask_job],
    resources=resources,
)
