"""Dask IOManager patterns for distributed clusters.

This module explores the architectural tension between:
1. Dagster IOManagers (which typically load data into memory)
2. Dask distributed computing (which keeps data across workers)

KEY INSIGHT: There are THREE different patterns, each for different use cases.
"""

import os
from typing import Union

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


# =============================================================================
# PATTERN 1: Lazy Reference IOManager (Best for Large Distributed Data)
# =============================================================================
# This IOManager does NOT load data into memory. Instead, it returns a LAZY
# Dask DataFrame that reads from storage when operations are performed.
# The data stays distributed across the Dask cluster.


class DaskLazyIOManager(ConfigurableIOManager):
    """IOManager that keeps data distributed - never loads into Dagster memory.

    This is the RIGHT pattern for large-scale distributed processing:
    - handle_output: Writes lazy Dask DataFrame to storage (triggers compute)
    - load_input: Returns a LAZY Dask DataFrame (no data loaded yet)

    The data flows through storage (Parquet), not through Dagster's memory.
    Dask workers read/write directly from/to storage.

    Use this when:
    - Data is too large to fit in memory
    - You want true distributed processing
    - You have a shared storage system (S3, GCS, HDFS, NFS)
    """

    base_path: str = Field(description="Base path for parquet storage (can be s3://, gs://, etc.)")

    def _get_path(self, context: Union[InputContext, OutputContext]) -> str:
        if context.has_asset_key:
            asset_key_path = "/".join(context.asset_key.path)
        else:
            asset_key_path = context.name
        return os.path.join(self.base_path, asset_key_path)

    def handle_output(self, context: OutputContext, obj: dd.DataFrame) -> None:
        """Write Dask DataFrame to storage.

        This DOES trigger computation, but the computation happens on the
        Dask cluster, not in the Dagster process. Each Dask worker writes
        its partition directly to storage.
        """
        if obj is None:
            return

        path = self._get_path(context)

        # Extract schema BEFORE writing (uses _meta, no computation)
        schema_columns = [
            TableColumn(name=str(col), type=str(dtype))
            for col, dtype in obj.dtypes.items()
        ]

        # Write to parquet - Dask workers write directly to storage
        # This triggers computation but data flows: Dask workers → Storage
        # NOT: Dask workers → Dagster process → Storage
        obj.to_parquet(path, engine="pyarrow", write_index=False)

        # Get row count efficiently from written parquet metadata
        # This reads parquet metadata, not the actual data
        written_ddf = dd.read_parquet(path)
        row_count = len(written_ddf)  # This does require a scan

        context.add_output_metadata({
            "dagster/column_schema": MetadataValue.table_schema(TableSchema(columns=schema_columns)),
            "dagster/row_count": row_count,
            "path": MetadataValue.path(path),
            "npartitions": obj.npartitions,
            "pattern": "lazy - data stays distributed",
        })

    def load_input(self, context: InputContext) -> dd.DataFrame:
        """Return a LAZY Dask DataFrame - no data loaded into memory!

        This returns a lazy reference. The actual data loading happens
        when operations are performed, and it happens on Dask workers,
        not in the Dagster process.
        """
        path = self._get_path(context)
        # This returns immediately - no data loaded yet!
        # Data will be read by Dask workers when .compute() is called
        return dd.read_parquet(path)


# =============================================================================
# PATTERN 2: Materialized IOManager (For Small-Medium Data)
# =============================================================================
# This IOManager DOES load data into memory, which is fine for smaller datasets
# where you want the simplicity of having data in the Dagster process.


class DaskMaterializingIOManager(ConfigurableIOManager):
    """IOManager that materializes Dask DataFrames to Pandas.

    This pattern:
    - handle_output: Calls .compute() to materialize, then saves
    - load_input: Returns a Pandas DataFrame (in memory)

    Use this when:
    - Data fits in memory
    - You want simpler debugging (data is in memory)
    - Downstream assets expect Pandas DataFrames
    """

    base_path: str = Field(description="Base path for parquet storage")

    def _get_path(self, context: Union[InputContext, OutputContext]) -> str:
        if context.has_asset_key:
            return os.path.join(self.base_path, "/".join(context.asset_key.path) + ".parquet")
        return os.path.join(self.base_path, context.name + ".parquet")

    def handle_output(self, context: OutputContext, obj: Union[dd.DataFrame, pd.DataFrame]) -> None:
        if obj is None:
            return

        path = self._get_path(context)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # Materialize if Dask DataFrame
        if isinstance(obj, dd.DataFrame):
            pdf = obj.compute()  # Data flows INTO Dagster process memory
        else:
            pdf = obj

        # Extract schema from pandas
        schema_columns = [
            TableColumn(name=str(col), type=str(dtype))
            for col, dtype in pdf.dtypes.items()
        ]

        pdf.to_parquet(path, engine="pyarrow", index=False)

        context.add_output_metadata({
            "dagster/column_schema": MetadataValue.table_schema(TableSchema(columns=schema_columns)),
            "dagster/row_count": len(pdf),
            "path": MetadataValue.path(path),
            "pattern": "materialized - data loaded into memory",
        })

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load as Pandas DataFrame (in memory)."""
        return pd.read_parquet(self._get_path(context))


# =============================================================================
# PATTERN 3: Hybrid IOManager (Configurable per-asset)
# =============================================================================
# Allows choosing lazy vs materialized on a per-asset basis via metadata.


class DaskHybridIOManager(ConfigurableIOManager):
    """IOManager that can work either lazily or materialized.

    Configure per-asset using metadata:
        @asset(metadata={"dask_mode": "lazy"})      # Keep distributed
        @asset(metadata={"dask_mode": "materialize"}) # Load into memory

    Default is "lazy" for true distributed processing.
    """

    base_path: str = Field(description="Base path for storage")
    default_mode: str = Field(default="lazy", description="Default mode: 'lazy' or 'materialize'")

    def _get_path(self, context: Union[InputContext, OutputContext]) -> str:
        if context.has_asset_key:
            return os.path.join(self.base_path, "/".join(context.asset_key.path))
        return os.path.join(self.base_path, context.name)

    def _get_mode(self, context: Union[InputContext, OutputContext]) -> str:
        """Get mode from asset metadata or use default."""
        if context.has_asset_key and context.definition_metadata:
            return context.definition_metadata.get("dask_mode", self.default_mode)
        return self.default_mode

    def handle_output(self, context: OutputContext, obj: dd.DataFrame) -> None:
        if obj is None:
            return

        path = self._get_path(context)
        mode = self._get_mode(context)

        # Schema extraction (always works without compute)
        schema_columns = [
            TableColumn(name=str(col), type=str(dtype))
            for col, dtype in obj.dtypes.items()
        ]

        if mode == "materialize":
            os.makedirs(path, exist_ok=True)
            pdf = obj.compute()
            pdf.to_parquet(os.path.join(path, "data.parquet"), index=False)
            row_count = len(pdf)
        else:
            obj.to_parquet(path, engine="pyarrow", write_index=False)
            row_count = len(dd.read_parquet(path))

        context.add_output_metadata({
            "dagster/column_schema": MetadataValue.table_schema(TableSchema(columns=schema_columns)),
            "dagster/row_count": row_count,
            "path": MetadataValue.path(path),
            "mode": mode,
        })

    def load_input(self, context: InputContext) -> Union[dd.DataFrame, pd.DataFrame]:
        path = self._get_path(context)
        mode = self._get_mode(context)

        if mode == "materialize":
            return pd.read_parquet(os.path.join(path, "data.parquet"))
        else:
            return dd.read_parquet(path)  # Lazy!


# =============================================================================
# SUMMARY: When to Use Each Pattern
# =============================================================================
#
# | Pattern      | Data Size | Data Location        | Schema Extraction |
# |--------------|-----------|----------------------|-------------------|
# | Lazy         | Large     | Stays on Dask cluster| Yes (from _meta)  |
# | Materializing| Small     | Loads into Dagster   | Yes (from pandas) |
# | Hybrid       | Mixed     | Configurable         | Yes               |
#
# For TRUE distributed processing with large data:
#   → Use DaskLazyIOManager
#   → Data flows: Storage ↔ Dask Workers (never through Dagster)
#   → Schema still extracted from _meta (no compute needed)
#
# =============================================================================
