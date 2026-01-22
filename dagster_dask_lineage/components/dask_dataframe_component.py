"""Dask DataFrame Component for Dagster.

This component provides a reusable pattern for creating Dask-based data pipelines
that automatically extract and surface:
- Column schema (dagster/column_schema)
- Row count (dagster/row_count)
- Partition information

The component can be configured via YAML and supports both demo mode (for local
development without a cluster) and production mode (connecting to a real Dask cluster).

Usage:
    1. Create a component instance YAML
    2. The component auto-generates assets with schema/row count metadata
    3. Data stays distributed (lazy IOManager pattern for large data)
"""

from abc import abstractmethod
from contextlib import contextmanager
from typing import Any, Callable, Iterator, Literal, Optional, Sequence, Union

import dask.dataframe as dd
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    Definitions,
    MaterializeResult,
    MetadataValue,
    TableColumn,
    TableSchema,
    asset,
    multi_asset,
    AssetSpec,
)
from dagster._core.definitions.asset_dep import AssetDep

try:
    from dagster import Component, ComponentLoadContext, Model, Resolvable
    HAS_COMPONENTS = True
except ImportError:
    HAS_COMPONENTS = False
    # Fallback for older Dagster versions
    from pydantic import BaseModel as Model
    Component = object
    Resolvable = object
    ComponentLoadContext = Any

from dask.distributed import Client, LocalCluster
from pydantic import Field


# =============================================================================
# Helper Functions for Metadata Extraction
# =============================================================================


def extract_dask_schema(ddf: dd.DataFrame) -> TableSchema:
    """Extract TableSchema from a Dask DataFrame's _meta attribute.

    This does NOT trigger computation - it uses Dask's metadata.
    """
    columns = [
        TableColumn(name=str(col), type=str(dtype))
        for col, dtype in ddf.dtypes.items()
    ]
    return TableSchema(columns=columns)


def create_dask_metadata(
    ddf: dd.DataFrame,
    compute_row_count: bool = True,
    extra_metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Create comprehensive metadata for a Dask DataFrame.

    Args:
        ddf: Dask DataFrame
        compute_row_count: If True, compute row count (triggers computation)
        extra_metadata: Additional metadata to include

    Returns:
        Metadata dict suitable for MaterializeResult
    """
    metadata: dict[str, Any] = {
        "dagster/column_schema": MetadataValue.table_schema(extract_dask_schema(ddf)),
        "npartitions": ddf.npartitions,
    }

    if compute_row_count:
        metadata["dagster/row_count"] = len(ddf)

    if extra_metadata:
        metadata.update(extra_metadata)

    return metadata


# =============================================================================
# Dask Cluster Configuration
# =============================================================================


class DaskClusterConfig(Model if HAS_COMPONENTS else object):
    """Configuration for Dask cluster connection."""

    cluster_type: Literal["local", "existing", "yarn", "kubernetes"] = Field(
        default="local",
        description="Type of Dask cluster to use",
    )
    scheduler_address: Optional[str] = Field(
        default=None,
        description="Address of existing Dask scheduler (e.g., 'tcp://scheduler:8786')",
    )
    n_workers: int = Field(
        default=4,
        description="Number of workers for local cluster",
    )
    threads_per_worker: int = Field(
        default=2,
        description="Threads per worker",
    )
    memory_limit: str = Field(
        default="2GB",
        description="Memory limit per worker",
    )


# =============================================================================
# Asset Configuration for Component
# =============================================================================


class DaskAssetConfig(Model if HAS_COMPONENTS else object):
    """Configuration for a single Dask asset within the component."""

    name: str = Field(description="Asset name")
    key_prefix: Optional[list[str]] = Field(
        default=None,
        description="Optional key prefix for the asset",
    )
    deps: Optional[list[str]] = Field(
        default=None,
        description="Asset dependencies (as asset key strings)",
    )
    description: Optional[str] = Field(
        default=None,
        description="Asset description",
    )
    group_name: Optional[str] = Field(
        default=None,
        description="Asset group name",
    )
    compute_row_count: bool = Field(
        default=True,
        description="Whether to compute and include row count in metadata",
    )


# =============================================================================
# Dask DataFrame Component
# =============================================================================


if HAS_COMPONENTS:
    class DaskDataFrameComponent(Component, Model, Resolvable):
        """Dagster Component for Dask DataFrame pipelines with automatic metadata.

        This component:
        1. Manages Dask cluster connection (local or distributed)
        2. Automatically extracts column schema from ddf._meta
        3. Optionally computes row count
        4. Supports demo mode for local development

        The component generates assets that return MaterializeResult with
        comprehensive metadata, making schema and row count visible in the
        Dagster UI.

        Example YAML:
            type: dagster_dask_lineage.components.DaskDataFrameComponent
            attributes:
              demo_mode: true
              cluster:
                cluster_type: local
                n_workers: 2
              assets:
                - name: raw_data
                  description: Raw input data
                - name: processed_data
                  deps: ["raw_data"]
                  description: Processed output
        """

        demo_mode: bool = Field(
            default=False,
            description="If True, use mock data for local development",
        )
        cluster: DaskClusterConfig = Field(
            default_factory=DaskClusterConfig,
            description="Dask cluster configuration",
        )
        assets: list[DaskAssetConfig] = Field(
            default_factory=list,
            description="List of assets to generate",
        )
        base_path: str = Field(
            default="/tmp/dagster_dask_component",
            description="Base path for data storage",
        )
        compute_row_count: bool = Field(
            default=True,
            description="Default setting for row count computation",
        )

        @contextmanager
        def get_dask_client(self) -> Iterator[Client]:
            """Get a Dask client based on configuration."""
            config = self.cluster

            if config.cluster_type == "local":
                cluster = LocalCluster(
                    n_workers=config.n_workers,
                    threads_per_worker=config.threads_per_worker,
                    memory_limit=config.memory_limit,
                )
                client = Client(cluster)
                try:
                    yield client
                finally:
                    client.close()
                    cluster.close()
            elif config.cluster_type == "existing":
                if not config.scheduler_address:
                    raise ValueError("scheduler_address required for existing cluster")
                client = Client(config.scheduler_address)
                try:
                    yield client
                finally:
                    client.close()
            else:
                raise ValueError(f"Unsupported cluster type: {config.cluster_type}")

        def _create_demo_asset(
            self,
            asset_config: DaskAssetConfig,
        ) -> AssetsDefinition:
            """Create a demo mode asset with mock data."""

            key = AssetKey(asset_config.key_prefix + [asset_config.name] if asset_config.key_prefix else [asset_config.name])
            deps = [AssetDep(d) for d in (asset_config.deps or [])]

            @asset(
                key=key,
                deps=deps,
                description=asset_config.description or f"Demo asset: {asset_config.name}",
                group_name=asset_config.group_name or "dask_demo",
                kinds={"dask", "demo"},
            )
            def demo_asset(context: AssetExecutionContext) -> MaterializeResult:
                import numpy as np

                context.log.info(f"Demo mode: Generating mock data for {asset_config.name}")

                # Generate mock data
                np.random.seed(42)
                n_rows = 1000

                pdf = pd.DataFrame({
                    "id": range(n_rows),
                    "value": np.random.randn(n_rows),
                    "category": np.random.choice(["A", "B", "C"], n_rows),
                })

                ddf = dd.from_pandas(pdf, npartitions=2)

                # Auto-extract metadata
                metadata = create_dask_metadata(
                    ddf,
                    compute_row_count=asset_config.compute_row_count,
                    extra_metadata={"demo_mode": True},
                )

                return MaterializeResult(metadata=metadata)

            # Rename the function to match asset name
            demo_asset.__name__ = asset_config.name
            return demo_asset

        def _create_production_asset(
            self,
            asset_config: DaskAssetConfig,
        ) -> AssetsDefinition:
            """Create a production asset that connects to real Dask cluster."""

            component = self  # Capture for closure
            key = AssetKey(asset_config.key_prefix + [asset_config.name] if asset_config.key_prefix else [asset_config.name])
            deps = [AssetDep(d) for d in (asset_config.deps or [])]

            @asset(
                key=key,
                deps=deps,
                description=asset_config.description or f"Dask asset: {asset_config.name}",
                group_name=asset_config.group_name or "dask_production",
                kinds={"dask"},
            )
            def production_asset(context: AssetExecutionContext) -> MaterializeResult:
                context.log.info(f"Production mode: Processing {asset_config.name}")

                with component.get_dask_client() as client:
                    context.log.info(f"Connected to Dask cluster: {client.dashboard_link}")

                    # In production, you would:
                    # 1. Read from your data source
                    # 2. Process with Dask
                    # 3. Write to your data sink

                    # Placeholder implementation - override in subclass
                    import numpy as np
                    np.random.seed(42)
                    n_rows = 10000

                    pdf = pd.DataFrame({
                        "id": range(n_rows),
                        "value": np.random.randn(n_rows),
                        "category": np.random.choice(["A", "B", "C"], n_rows),
                        "timestamp": pd.date_range("2024-01-01", periods=n_rows, freq="min"),
                    })

                    ddf = dd.from_pandas(pdf, npartitions=4)

                    # Auto-extract metadata
                    metadata = create_dask_metadata(
                        ddf,
                        compute_row_count=asset_config.compute_row_count,
                        extra_metadata={
                            "cluster_dashboard": client.dashboard_link or "N/A",
                        },
                    )

                    return MaterializeResult(metadata=metadata)

            production_asset.__name__ = asset_config.name
            return production_asset

        def build_defs(self, context: ComponentLoadContext) -> Definitions:
            """Build Dagster definitions for this component.

            This method:
            1. Creates assets based on configuration
            2. Each asset automatically extracts schema and row count
            3. Demo mode uses mock data, production connects to cluster
            """
            assets = []

            for asset_config in self.assets:
                if self.demo_mode:
                    assets.append(self._create_demo_asset(asset_config))
                else:
                    assets.append(self._create_production_asset(asset_config))

            return Definitions(assets=assets)


# =============================================================================
# Fallback for older Dagster versions without Component support
# =============================================================================


class DaskDataFrameComponentLegacy:
    """Legacy version for Dagster versions without Component support.

    Use this as a factory to create assets with automatic metadata extraction.
    """

    def __init__(
        self,
        demo_mode: bool = False,
        cluster_config: Optional[DaskClusterConfig] = None,
        compute_row_count: bool = True,
    ):
        self.demo_mode = demo_mode
        self.cluster_config = cluster_config or DaskClusterConfig()
        self.compute_row_count = compute_row_count

    @contextmanager
    def get_client(self) -> Iterator[Client]:
        """Get a Dask client."""
        config = self.cluster_config

        if config.cluster_type == "local":
            cluster = LocalCluster(
                n_workers=config.n_workers,
                threads_per_worker=config.threads_per_worker,
                memory_limit=config.memory_limit,
            )
            client = Client(cluster)
            try:
                yield client
            finally:
                client.close()
                cluster.close()
        else:
            client = Client(config.scheduler_address)
            try:
                yield client
            finally:
                client.close()

    def create_asset(
        self,
        name: str,
        process_fn: Callable[[AssetExecutionContext, Client], dd.DataFrame],
        deps: Optional[list[str]] = None,
        description: Optional[str] = None,
        group_name: Optional[str] = None,
    ) -> AssetsDefinition:
        """Create an asset with automatic metadata extraction.

        Args:
            name: Asset name
            process_fn: Function that takes (context, client) and returns a Dask DataFrame
            deps: Asset dependencies
            description: Asset description
            group_name: Asset group name

        Returns:
            AssetsDefinition with automatic schema/row count metadata
        """
        component = self

        @asset(
            name=name,
            deps=deps or [],
            description=description,
            group_name=group_name,
            kinds={"dask"},
        )
        def dask_asset(context: AssetExecutionContext) -> MaterializeResult:
            with component.get_client() as client:
                # Call user's processing function
                ddf = process_fn(context, client)

                # Auto-extract metadata
                metadata = create_dask_metadata(
                    ddf,
                    compute_row_count=component.compute_row_count,
                )

                return MaterializeResult(metadata=metadata)

        return dask_asset
