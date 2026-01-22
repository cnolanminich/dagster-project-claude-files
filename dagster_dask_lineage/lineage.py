"""Column-level lineage utilities for Dask DataFrames.

This module provides utilities to extract schema information and attempt to
infer column-level lineage from Dask DataFrames. It investigates what metadata
Dask provides that could be used for lineage tracking.

Key Findings on Dask's Lineage Capabilities:
============================================

1. SCHEMA INFORMATION (WELL SUPPORTED):
   - Dask provides excellent schema introspection via the `_meta` attribute
   - Column names, dtypes, and index information are readily available
   - This works without computing the full DataFrame

2. ROW COUNT (REQUIRES COMPUTATION):
   - Row counts require either full computation (len()) or partition-level counts
   - Dask provides `npartitions` which gives partition count without computation
   - Exact row count requires `len(ddf)` which triggers computation

3. COLUMN-LEVEL LINEAGE (LIMITED SUPPORT):
   - Dask does NOT natively track column-level lineage
   - The task graph tracks operations but not column transformations
   - To get column lineage, you must:
     a) Parse the task graph manually (complex, not recommended)
     b) Track transformations explicitly in your code
     c) Use a wrapper that records column dependencies

4. WHAT'S AVAILABLE FROM DASK:
   - `ddf._meta`: Empty DataFrame with correct schema
   - `ddf.columns`: Column names
   - `ddf.dtypes`: Column data types
   - `ddf.index`: Index information
   - `ddf.npartitions`: Number of partitions
   - `ddf.__dask_graph__()`: Task graph (low-level, not column-aware)

5. RECOMMENDED APPROACH FOR DAGSTER:
   - Use `_meta` to extract schema for TableSchema
   - Compute row count during materialization
   - Track column lineage EXPLICITLY by recording which source columns
     are used to derive each output column
"""

from typing import Any, Optional

import dask.dataframe as dd
import pandas as pd
from dagster import (
    AssetKey,
    MaterializeResult,
    MetadataValue,
    TableColumn,
    TableColumnDep,
    TableColumnLineage,
    TableSchema,
)


def extract_schema_from_dask(ddf: dd.DataFrame) -> TableSchema:
    """Extract TableSchema from a Dask DataFrame's metadata.

    This uses Dask's `_meta` attribute which contains an empty pandas DataFrame
    with the correct column names and dtypes. This is available without
    computing the full DataFrame.

    Args:
        ddf: A Dask DataFrame

    Returns:
        A Dagster TableSchema object describing the DataFrame's columns
    """
    meta = ddf._meta
    columns = []

    for col_name in meta.columns:
        dtype = meta[col_name].dtype
        columns.append(
            TableColumn(
                name=str(col_name),
                type=str(dtype),
                description=f"Column '{col_name}' with dtype {dtype}",
            )
        )

    return TableSchema(columns=columns)


def extract_schema_from_pandas(df: pd.DataFrame) -> TableSchema:
    """Extract TableSchema from a Pandas DataFrame.

    Args:
        df: A Pandas DataFrame

    Returns:
        A Dagster TableSchema object describing the DataFrame's columns
    """
    columns = []

    for col_name in df.columns:
        dtype = df[col_name].dtype
        columns.append(
            TableColumn(
                name=str(col_name),
                type=str(dtype),
                description=f"Column '{col_name}' with dtype {dtype}",
            )
        )

    return TableSchema(columns=columns)


def get_dask_metadata_without_compute(ddf: dd.DataFrame) -> dict[str, Any]:
    """Extract all available metadata from a Dask DataFrame WITHOUT computing.

    This demonstrates what information Dask provides natively without
    triggering computation of the full DataFrame.

    Args:
        ddf: A Dask DataFrame

    Returns:
        Dictionary containing available metadata
    """
    return {
        "columns": list(ddf.columns),
        "dtypes": {str(k): str(v) for k, v in ddf.dtypes.items()},
        "npartitions": ddf.npartitions,
        "index_name": ddf.index.name,
        "index_dtype": str(ddf.index.dtype),
        "known_divisions": ddf.known_divisions,
        "divisions": list(ddf.divisions) if ddf.known_divisions else None,
    }


def get_row_count(ddf: dd.DataFrame) -> int:
    """Get the row count of a Dask DataFrame.

    WARNING: This triggers computation! Use sparingly in production.

    Args:
        ddf: A Dask DataFrame

    Returns:
        The total number of rows
    """
    return len(ddf)


def get_partition_row_counts(ddf: dd.DataFrame) -> list[int]:
    """Get row counts per partition.

    This can be more efficient than getting total row count as it
    allows parallel computation across partitions.

    Args:
        ddf: A Dask DataFrame

    Returns:
        List of row counts, one per partition
    """
    return ddf.map_partitions(len).compute().tolist()


class ColumnLineageTracker:
    """A utility class to explicitly track column-level lineage.

    Since Dask doesn't natively track column lineage, this class provides
    a way to explicitly record column dependencies as you build your
    data transformations.

    Example:
        ```python
        tracker = ColumnLineageTracker()

        # Record that 'full_name' depends on 'first_name' and 'last_name' from 'users'
        tracker.add_column_dep(
            output_column="full_name",
            source_asset="users",
            source_columns=["first_name", "last_name"]
        )

        # Later, get the lineage for Dagster
        lineage = tracker.get_table_column_lineage()
        ```
    """

    def __init__(self) -> None:
        self._deps: dict[str, list[TableColumnDep]] = {}

    def add_column_dep(
        self,
        output_column: str,
        source_asset: str | AssetKey,
        source_columns: list[str],
    ) -> None:
        """Record that an output column depends on source columns.

        Args:
            output_column: Name of the output column being created
            source_asset: Asset key or string name of the source asset
            source_columns: List of column names in the source that this output depends on
        """
        if isinstance(source_asset, str):
            source_asset = AssetKey(source_asset)

        if output_column not in self._deps:
            self._deps[output_column] = []

        for source_col in source_columns:
            self._deps[output_column].append(
                TableColumnDep(asset_key=source_asset, column_name=source_col)
            )

    def add_passthrough_columns(
        self,
        columns: list[str],
        source_asset: str | AssetKey,
    ) -> None:
        """Record that columns are passed through unchanged from source.

        This is a convenience method for columns that are simply copied
        from the source without transformation.

        Args:
            columns: List of column names being passed through
            source_asset: Asset key or string name of the source asset
        """
        for col in columns:
            self.add_column_dep(col, source_asset, [col])

    def get_table_column_lineage(self) -> TableColumnLineage:
        """Get the TableColumnLineage object for Dagster.

        Returns:
            A TableColumnLineage object that can be included in asset metadata
        """
        return TableColumnLineage(deps_by_column=self._deps)

    def get_lineage_metadata(self) -> dict[str, Any]:
        """Get lineage as metadata dict ready for MaterializeResult.

        Returns:
            Dictionary with 'dagster/column_lineage' key
        """
        return {"dagster/column_lineage": self.get_table_column_lineage()}


def create_materialization_metadata(
    df: pd.DataFrame | dd.DataFrame,
    row_count: Optional[int] = None,
    lineage_tracker: Optional[ColumnLineageTracker] = None,
) -> dict[str, Any]:
    """Create comprehensive metadata for a DataFrame materialization.

    This combines schema, row count, and optional column lineage into
    a metadata dictionary suitable for MaterializeResult.

    Args:
        df: The DataFrame (Pandas or Dask)
        row_count: Optional pre-computed row count. If not provided and df is
                   a Dask DataFrame, will trigger computation.
        lineage_tracker: Optional ColumnLineageTracker with recorded dependencies

    Returns:
        Metadata dictionary for MaterializeResult
    """
    # Extract schema based on DataFrame type
    if isinstance(df, dd.DataFrame):
        schema = extract_schema_from_dask(df)
        if row_count is None:
            row_count = len(df)
    else:
        schema = extract_schema_from_pandas(df)
        if row_count is None:
            row_count = len(df)

    metadata: dict[str, Any] = {
        "dagster/column_schema": schema,
        "dagster/row_count": row_count,
    }

    if lineage_tracker:
        metadata.update(lineage_tracker.get_lineage_metadata())

    return metadata


def inspect_dask_task_graph(ddf: dd.DataFrame) -> dict[str, Any]:
    """Inspect the Dask task graph for debugging/analysis.

    This demonstrates what information is available in the task graph.
    Note: The task graph does NOT contain column-level lineage information.
    It tracks computational dependencies at the partition/chunk level.

    Args:
        ddf: A Dask DataFrame

    Returns:
        Dictionary with task graph information
    """
    graph = ddf.__dask_graph__()
    keys = list(ddf.__dask_keys__())

    return {
        "total_tasks": len(graph),
        "output_keys_count": len(keys),
        "sample_output_key": str(keys[0]) if keys else None,
        "task_types": list(set(type(v).__name__ for v in graph.values())),
        # Note: Actual task contents are complex tuples/functions
        # They don't contain column-level dependency information
        "note": "Task graph tracks partition-level dependencies, not column-level",
    }
