"""Jobs demonstrating the official dagster-dask executor.

This module shows how to use the ACTUAL dagster-dask library's `dask_executor`
to distribute execution steps across a Dask cluster.

KEY INSIGHT: You can use BOTH approaches together!
=================================================

1. `dask_executor` (from dagster-dask library):
   - Distributes EXECUTION STEPS (ops/assets) across a Dask cluster
   - Each asset runs on a different worker

2. Dask DataFrames WITHIN assets:
   - Use Dask for parallel computation within each asset
   - Extract schema from ddf._meta
   - Track column lineage explicitly

These compose together: dask_executor distributes your assets across workers,
and within each worker, you can still use Dask DataFrames for data processing
and extract schema/lineage metadata.
"""

from dagster import AssetSelection, define_asset_job
from dagster_dask import dask_executor

# Job that uses the official dask_executor from dagster-dask
# This distributes execution of different assets/ops across Dask workers
dask_distributed_job = define_asset_job(
    name="dask_distributed_job",
    selection=AssetSelection.groups("raw", "cleaned"),
    executor_def=dask_executor.configured({
        "cluster": {
            "local": {
                "n_workers": 4,
                "threads_per_worker": 2,
            }
        }
    }),
    description=(
        "Job that uses dagster-dask's dask_executor to distribute "
        "asset materialization steps across a Dask cluster. Each asset "
        "runs on a separate Dask worker."
    ),
)

# Job using the default executor (for comparison)
# This runs assets sequentially or with Dagster's built-in parallelism
default_executor_job = define_asset_job(
    name="default_executor_job",
    selection=AssetSelection.groups("raw", "cleaned"),
    description=(
        "Job using the default executor. Assets still use DaskResource "
        "internally for parallel DataFrame processing, but execution "
        "steps are not distributed across a Dask cluster."
    ),
)

# =============================================================================
# COMBINED APPROACH: dask_executor + schema extraction + lineage tracking
# =============================================================================
# This job demonstrates using BOTH:
# - dask_executor to distribute assets across workers
# - Dask DataFrames within each asset for parallel processing
# - Schema extraction from ddf._meta
# - Explicit column lineage tracking

combined_dask_job = define_asset_job(
    name="combined_dask_job",
    selection=AssetSelection.groups("combined_example"),
    executor_def=dask_executor.configured({
        "cluster": {
            "local": {
                "n_workers": 2,
                "threads_per_worker": 2,
            }
        }
    }),
    description=(
        "Demonstrates BOTH approaches together:\n"
        "1. dask_executor distributes assets across Dask workers\n"
        "2. Each asset uses Dask DataFrames internally\n"
        "3. Schema extracted from ddf._meta\n"
        "4. Column lineage tracked explicitly\n\n"
        "This shows they compose well - the executor handles distribution, "
        "while internal Dask usage handles parallel data processing."
    ),
)
