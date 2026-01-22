"""Dagster-Dask integration with column schema and lineage tracking.

This package provides utilities that could be contributed to dagster-dask:

1. IOManagers with automatic metadata extraction:
   - DaskParquetIOManager: Auto-extracts schema/row count for all assets
   - DaskPandasParquetIOManager: Handles both Dask and Pandas DataFrames

2. Utility functions (like dagster-pandas):
   - create_table_schema_metadata_from_dask_dataframe()
   - create_dask_dataframe_metadata()

3. Column lineage tracking:
   - ColumnLineageTracker: Explicit column-level lineage recording
"""

from dagster_dask_lineage.io_manager import (
    DaskPandasParquetIOManager,
    DaskParquetIOManager,
)
from dagster_dask_lineage.lineage import (
    ColumnLineageTracker,
    create_materialization_metadata,
    extract_schema_from_dask,
    extract_schema_from_pandas,
    get_dask_metadata_without_compute,
    get_partition_row_counts,
    get_row_count,
)
from dagster_dask_lineage.metadata_utils import (
    create_dask_dataframe_metadata,
    create_table_schema_metadata_from_dask_dataframe,
    get_dask_schema_without_compute,
)
from dagster_dask_lineage.resources import DaskResource

__all__ = [
    # IOManagers (automatic metadata extraction)
    "DaskParquetIOManager",
    "DaskPandasParquetIOManager",
    # Utility functions (like dagster-pandas)
    "create_table_schema_metadata_from_dask_dataframe",
    "create_dask_dataframe_metadata",
    "get_dask_schema_without_compute",
    # Resources
    "DaskResource",
    # Lineage tracking
    "ColumnLineageTracker",
    "create_materialization_metadata",
    "extract_schema_from_dask",
    "extract_schema_from_pandas",
    "get_dask_metadata_without_compute",
    "get_row_count",
    "get_partition_row_counts",
]
