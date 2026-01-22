"""Dask DataFrame IOManager with automatic schema and row count extraction.

This module provides an IOManager that automatically extracts and attaches
schema and row count metadata when storing Dask DataFrames. This is the key
to making metadata extraction automatic for all assets.

Pattern reference: dagster-pandas, dagster-duckdb-pandas IOManagers
"""

import os
from typing import Any, Optional, Union

import dask.dataframe as dd
import pandas as pd
from dagster import (
    ConfigurableIOManager,
    InputContext,
    MetadataValue,
    OutputContext,
    TableColumn,
    TableSchema,
)
from pydantic import Field


class DaskParquetIOManager(ConfigurableIOManager):
    """IOManager that stores Dask DataFrames as Parquet and auto-extracts metadata.

    This IOManager automatically:
    1. Extracts column schema from ddf._meta (no computation needed)
    2. Computes and attaches row count
    3. Records partition information

    When you use this IOManager, ALL assets that return Dask DataFrames will
    automatically get schema and row count metadata attached.

    Example:
        ```python
        from dagster import Definitions, asset
        import dask.dataframe as dd

        @asset
        def my_dask_asset() -> dd.DataFrame:
            return dd.read_csv("data.csv")

        defs = Definitions(
            assets=[my_dask_asset],
            resources={
                "io_manager": DaskParquetIOManager(base_path="/data/warehouse"),
            },
        )
        ```

        The resulting asset will automatically have:
        - dagster/column_schema: TableSchema with all columns
        - dagster/row_count: Total number of rows
        - npartitions: Number of Dask partitions
    """

    base_path: str = Field(
        description="Base directory path for storing parquet files"
    )
    compute_row_count: bool = Field(
        default=True,
        description="Whether to compute row count (requires computation)",
    )

    def _get_path(self, context: Union[InputContext, OutputContext]) -> str:
        """Get the file path for an asset."""
        # Use asset key to create path
        if context.has_asset_key:
            asset_key_path = "/".join(context.asset_key.path)
        else:
            asset_key_path = context.name

        return os.path.join(self.base_path, f"{asset_key_path}.parquet")

    def handle_output(self, context: OutputContext, obj: dd.DataFrame) -> None:
        """Store the Dask DataFrame and automatically extract metadata.

        This is where the magic happens - metadata is extracted automatically
        for every asset that uses this IOManager.
        """
        if obj is None:
            return

        path = self._get_path(context)

        # Ensure directory exists
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # Extract schema BEFORE computing (uses _meta, no computation needed)
        schema_columns = []
        for col_name, dtype in obj.dtypes.items():
            schema_columns.append(
                TableColumn(name=str(col_name), type=str(dtype))
            )
        table_schema = TableSchema(columns=schema_columns)

        # Get partition count (no computation needed)
        npartitions = obj.npartitions

        # Write to parquet (this triggers computation)
        obj.to_parquet(path, engine="pyarrow", write_index=False)

        # Compute row count if enabled
        # Note: Since we just wrote to parquet, we can read the count efficiently
        if self.compute_row_count:
            # Read back just to get count - more efficient than len(obj) before write
            row_count = len(dd.read_parquet(path))
        else:
            row_count = None

        # Automatically attach metadata to the output
        metadata = {
            "dagster/column_schema": MetadataValue.table_schema(table_schema),
            "path": MetadataValue.path(path),
            "npartitions": npartitions,
        }

        if row_count is not None:
            metadata["dagster/row_count"] = row_count

        context.add_output_metadata(metadata)

        context.log.info(
            f"Stored Dask DataFrame to {path} "
            f"({len(schema_columns)} columns, {npartitions} partitions)"
        )

    def load_input(self, context: InputContext) -> dd.DataFrame:
        """Load the Dask DataFrame from parquet."""
        path = self._get_path(context)
        context.log.info(f"Loading Dask DataFrame from {path}")
        return dd.read_parquet(path)


class DaskPandasParquetIOManager(ConfigurableIOManager):
    """IOManager that handles both Dask and Pandas DataFrames with auto-metadata.

    This IOManager:
    - Accepts both Dask DataFrames and Pandas DataFrames as output
    - Always loads as Dask DataFrame for consistency
    - Automatically extracts schema and row count for both types

    This is useful when some assets produce Pandas and others produce Dask,
    but you want consistent metadata extraction for all.
    """

    base_path: str = Field(
        description="Base directory path for storing parquet files"
    )
    compute_row_count: bool = Field(
        default=True,
        description="Whether to compute row count",
    )
    default_npartitions: int = Field(
        default=4,
        description="Default partitions when converting Pandas to Dask",
    )

    def _get_path(self, context: Union[InputContext, OutputContext]) -> str:
        if context.has_asset_key:
            asset_key_path = "/".join(context.asset_key.path)
        else:
            asset_key_path = context.name
        return os.path.join(self.base_path, f"{asset_key_path}.parquet")

    def handle_output(
        self, context: OutputContext, obj: Union[dd.DataFrame, pd.DataFrame]
    ) -> None:
        """Store DataFrame and automatically extract metadata."""
        if obj is None:
            return

        path = self._get_path(context)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # Convert Pandas to Dask if needed
        if isinstance(obj, pd.DataFrame):
            ddf = dd.from_pandas(obj, npartitions=self.default_npartitions)
            was_pandas = True
        else:
            ddf = obj
            was_pandas = False

        # Extract schema (works the same for both)
        schema_columns = []
        for col_name, dtype in ddf.dtypes.items():
            schema_columns.append(
                TableColumn(name=str(col_name), type=str(dtype))
            )
        table_schema = TableSchema(columns=schema_columns)

        # Write and get row count
        ddf.to_parquet(path, engine="pyarrow", write_index=False)

        row_count = None
        if self.compute_row_count:
            if was_pandas:
                row_count = len(obj)  # Already in memory, no extra compute
            else:
                row_count = len(dd.read_parquet(path))

        # Attach metadata automatically
        metadata = {
            "dagster/column_schema": MetadataValue.table_schema(table_schema),
            "path": MetadataValue.path(path),
            "npartitions": ddf.npartitions,
            "source_type": "pandas" if was_pandas else "dask",
        }
        if row_count is not None:
            metadata["dagster/row_count"] = row_count

        context.add_output_metadata(metadata)

    def load_input(self, context: InputContext) -> dd.DataFrame:
        """Load as Dask DataFrame."""
        return dd.read_parquet(self._get_path(context))
