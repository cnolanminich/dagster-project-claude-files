"""Dagster-Dask metadata utilities - proposed additions to dagster-dask library.

This module provides utilities for automatic schema and row count extraction
from Dask DataFrames, following the pattern established by dagster-pandas.

These functions could be contributed to the dagster-dask library to provide
automatic metadata extraction for Dask DataFrames.

Reference: dagster-pandas.data_frame.create_table_schema_metadata_from_dataframe
"""

from typing import Any, Mapping, Optional, Sequence, Union

import dask.dataframe as dd
import pandas as pd
from dagster import (
    MetadataValue,
    TableColumn,
    TableSchema,
    TableSchemaMetadataValue,
)
from dagster._core.definitions.metadata import RawMetadataMapping


def create_table_schema_metadata_from_dask_dataframe(
    dask_df: dd.DataFrame,
) -> TableSchemaMetadataValue:
    """Create TableSchema metadata from a Dask DataFrame.

    This is the Dask equivalent of dagster-pandas's
    `create_table_schema_metadata_from_dataframe`. It extracts schema
    information from the Dask DataFrame's `_meta` attribute WITHOUT
    triggering computation.

    Args:
        dask_df: A Dask DataFrame

    Returns:
        A TableSchemaMetadataValue that can be attached to asset metadata

    Example:
        ```python
        from dagster import asset
        from dagster_dask import create_table_schema_metadata_from_dask_dataframe

        @asset
        def my_asset():
            ddf = dd.read_parquet("data.parquet")
            return MaterializeResult(
                metadata={
                    "dagster/column_schema": create_table_schema_metadata_from_dask_dataframe(ddf)
                }
            )
        ```
    """
    columns = []
    for col_name, dtype in dask_df.dtypes.items():
        columns.append(
            TableColumn(
                name=str(col_name),
                type=str(dtype),
            )
        )

    return MetadataValue.table_schema(TableSchema(columns=columns))


def create_dask_dataframe_metadata(
    dask_df: dd.DataFrame,
    include_row_count: bool = True,
    include_schema: bool = True,
    include_partition_info: bool = True,
    additional_metadata: Optional[RawMetadataMapping] = None,
) -> dict[str, Any]:
    """Create comprehensive metadata for a Dask DataFrame.

    This function extracts multiple pieces of metadata from a Dask DataFrame.
    Note that `include_row_count=True` will trigger computation to get the
    exact row count.

    Args:
        dask_df: A Dask DataFrame
        include_row_count: If True, compute and include row count (triggers computation)
        include_schema: If True, include column schema (no computation needed)
        include_partition_info: If True, include partition count (no computation needed)
        additional_metadata: Optional additional metadata to merge

    Returns:
        A dictionary of metadata suitable for MaterializeResult or add_output_metadata

    Example:
        ```python
        @asset
        def my_asset():
            ddf = dd.read_parquet("data.parquet")
            # Process...
            result = ddf.compute()

            return MaterializeResult(
                metadata=create_dask_dataframe_metadata(
                    ddf,
                    include_row_count=True,  # We already computed, so this is cheap
                )
            )
        ```
    """
    metadata: dict[str, Any] = {}

    if include_schema:
        metadata["dagster/column_schema"] = create_table_schema_metadata_from_dask_dataframe(
            dask_df
        )

    if include_row_count:
        # Note: This triggers computation!
        metadata["dagster/row_count"] = len(dask_df)

    if include_partition_info:
        metadata["npartitions"] = dask_df.npartitions
        metadata["known_divisions"] = dask_df.known_divisions

    if additional_metadata:
        metadata.update(additional_metadata)

    return metadata


def get_dask_schema_without_compute(dask_df: dd.DataFrame) -> TableSchema:
    """Extract TableSchema from a Dask DataFrame without computation.

    This uses the `_meta` attribute which contains schema information
    without needing to load the actual data.

    Args:
        dask_df: A Dask DataFrame

    Returns:
        A TableSchema object describing the DataFrame's columns
    """
    columns = []
    for col_name, dtype in dask_df.dtypes.items():
        columns.append(
            TableColumn(
                name=str(col_name),
                type=str(dtype),
            )
        )
    return TableSchema(columns=columns)
