"""Examples of using the DaskDataFrameComponent.

This file shows:
1. Using the legacy factory pattern (works with any Dagster version)
2. Extending the component for custom processing
3. Creating assets with automatic metadata extraction
"""

import dask.dataframe as dd
import numpy as np
import pandas as pd
from dagster import AssetExecutionContext, Definitions, asset

from dagster_dask_lineage.components import (
    DaskClusterConfig,
    DaskDataFrameComponentLegacy,
    create_dask_metadata,
    extract_dask_schema,
)


# =============================================================================
# Example 1: Using the Legacy Factory Pattern
# =============================================================================
# This works with any Dagster version and gives you full control over
# the processing logic while still getting automatic metadata extraction.


def create_orders_pipeline() -> list:
    """Create a pipeline using the legacy factory pattern."""

    # Create the component factory
    dask_factory = DaskDataFrameComponentLegacy(
        demo_mode=True,  # Use mock data
        cluster_config=DaskClusterConfig(
            cluster_type="local",
            n_workers=2,
        ),
        compute_row_count=True,
    )

    # Define processing functions
    def process_raw_orders(context: AssetExecutionContext, client) -> dd.DataFrame:
        """Generate raw orders data."""
        context.log.info("Generating raw orders data")

        np.random.seed(42)
        n_rows = 5000

        pdf = pd.DataFrame({
            "order_id": range(1, n_rows + 1),
            "customer_id": np.random.randint(1, 501, n_rows),
            "amount": np.round(np.random.uniform(10, 500, n_rows), 2),
            "status": np.random.choice(["pending", "completed", "cancelled"], n_rows),
        })

        return dd.from_pandas(pdf, npartitions=4)

    def process_order_summary(context: AssetExecutionContext, client) -> dd.DataFrame:
        """Aggregate orders by status."""
        context.log.info("Computing order summary")

        # In production, you'd read from the upstream asset
        np.random.seed(42)
        n_rows = 5000

        pdf = pd.DataFrame({
            "order_id": range(1, n_rows + 1),
            "customer_id": np.random.randint(1, 501, n_rows),
            "amount": np.round(np.random.uniform(10, 500, n_rows), 2),
            "status": np.random.choice(["pending", "completed", "cancelled"], n_rows),
        })

        ddf = dd.from_pandas(pdf, npartitions=4)

        # Aggregate by status
        summary = ddf.groupby("status").agg({
            "order_id": "count",
            "amount": ["sum", "mean"],
        }).reset_index()

        summary.columns = ["status", "order_count", "total_amount", "avg_amount"]

        return summary

    # Create assets with automatic metadata extraction
    raw_orders = dask_factory.create_asset(
        name="factory_raw_orders",
        process_fn=process_raw_orders,
        description="Raw orders with auto-extracted schema",
        group_name="factory_example",
    )

    order_summary = dask_factory.create_asset(
        name="factory_order_summary",
        process_fn=process_order_summary,
        deps=["factory_raw_orders"],
        description="Order summary with auto-extracted schema",
        group_name="factory_example",
    )

    return [raw_orders, order_summary]


# =============================================================================
# Example 2: Direct Asset with Metadata Helpers
# =============================================================================
# If you want full control, use the helper functions directly.


@asset(
    group_name="direct_example",
    kinds={"dask"},
    description="Direct asset using metadata helpers",
)
def direct_dask_asset(context: AssetExecutionContext):
    """Example of using metadata helpers directly."""
    from dagster import MaterializeResult

    np.random.seed(123)
    n_rows = 3000

    pdf = pd.DataFrame({
        "id": range(n_rows),
        "value": np.random.randn(n_rows),
        "category": np.random.choice(["X", "Y", "Z"], n_rows),
    })

    ddf = dd.from_pandas(pdf, npartitions=2)

    # Use the helper to create metadata automatically
    metadata = create_dask_metadata(
        ddf,
        compute_row_count=True,
        extra_metadata={
            "source": "direct_example",
            "custom_field": "any value you want",
        },
    )

    context.log.info(f"Schema: {[c.name for c in extract_dask_schema(ddf).columns]}")

    return MaterializeResult(metadata=metadata)


# =============================================================================
# Example 3: Custom Component Extension
# =============================================================================
# Extend the component for specific use cases.


class ETLDaskComponent(DaskDataFrameComponentLegacy):
    """Extended component for ETL pipelines.

    Adds:
    - Source/sink configuration
    - Data validation
    - Custom metadata
    """

    def __init__(
        self,
        source_path: str,
        sink_path: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.source_path = source_path
        self.sink_path = sink_path

    def create_etl_asset(
        self,
        name: str,
        transform_fn,
        **kwargs,
    ):
        """Create an ETL asset with source/sink handling."""

        component = self

        def etl_process(context: AssetExecutionContext, client) -> dd.DataFrame:
            context.log.info(f"Reading from {component.source_path}")

            # In production, read from source
            # ddf = dd.read_parquet(component.source_path)

            # For demo, create mock data
            np.random.seed(42)
            pdf = pd.DataFrame({
                "id": range(1000),
                "value": np.random.randn(1000),
            })
            ddf = dd.from_pandas(pdf, npartitions=2)

            # Apply user's transform
            result = transform_fn(ddf)

            context.log.info(f"Would write to {component.sink_path}")
            # result.to_parquet(component.sink_path)

            return result

        return self.create_asset(name=name, process_fn=etl_process, **kwargs)


# =============================================================================
# Definitions
# =============================================================================

# Create assets from factory
factory_assets = create_orders_pipeline()

defs = Definitions(
    assets=[
        *factory_assets,
        direct_dask_asset,
    ],
)
