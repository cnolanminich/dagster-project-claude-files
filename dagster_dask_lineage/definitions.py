"""Dagster definitions for the dagster-dask-lineage project.

This module configures the Dagster code location with:
- Dask resource for parallel DataFrame processing WITHIN assets
- Jobs using dagster-dask's dask_executor for distributed STEP execution
- Assets demonstrating schema and lineage tracking

TWO DASK INTEGRATION PATTERNS:
==============================
1. `dask_executor` (from dagster-dask): Distributes ops/steps across Dask workers
2. `DaskResource` (custom): Parallelizes computation within a single asset

This project demonstrates BOTH approaches.
"""

from dagster import Definitions, load_assets_from_modules

from dagster_dask_lineage import assets
from dagster_dask_lineage.jobs import dask_distributed_job, default_executor_job
from dagster_dask_lineage.resources import DaskResource

# Load all assets from the assets module
all_assets = load_assets_from_modules([assets])

# Configure resources
# Note: This DaskResource is our CUSTOM resource for in-asset parallelism
# It is NOT from the dagster-dask library
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
    jobs=[dask_distributed_job, default_executor_job],
    resources=resources,
)
