"""Jobs demonstrating the official dagster-dask executor.

This module shows how to use the ACTUAL dagster-dask library's `dask_executor`
to distribute execution steps across a Dask cluster.

IMPORTANT DISTINCTION:
======================
There are TWO ways to use Dask with Dagster:

1. `dask_executor` (from dagster-dask library):
   - Distributes EXECUTION STEPS (ops) across a Dask cluster
   - Each op runs on a different worker
   - Good for: many independent ops that can run in parallel

2. Custom `DaskResource` (what we built in resources.py):
   - Uses Dask for parallel computation WITHIN a single asset
   - The asset itself runs on one machine, but Dask parallelizes the work
   - Good for: large DataFrame processing within a single asset

This module demonstrates approach #1 using the official library.
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
