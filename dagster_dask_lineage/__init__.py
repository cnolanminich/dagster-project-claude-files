"""Dagster-Dask integration with column schema and lineage tracking."""

from dagster_dask_lineage.lineage import (
    ColumnLineageTracker,
    create_materialization_metadata,
    extract_schema_from_dask,
    extract_schema_from_pandas,
    get_dask_metadata_without_compute,
    get_partition_row_counts,
    get_row_count,
)
from dagster_dask_lineage.resources import DaskResource

__all__ = [
    "DaskResource",
    "ColumnLineageTracker",
    "create_materialization_metadata",
    "extract_schema_from_dask",
    "extract_schema_from_pandas",
    "get_dask_metadata_without_compute",
    "get_row_count",
    "get_partition_row_counts",
]
