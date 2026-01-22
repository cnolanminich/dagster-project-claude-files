"""Dagster definitions for the dagster-dask-lineage project.

This module configures the Dagster code location with:
- Dask resource for parallel DataFrame processing
- Assets demonstrating schema and lineage tracking
"""

from dagster import Definitions, load_assets_from_modules

from dagster_dask_lineage import assets
from dagster_dask_lineage.resources import DaskResource

# Load all assets from the assets module
all_assets = load_assets_from_modules([assets])

# Configure resources
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
    resources=resources,
)
