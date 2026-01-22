"""Assets that work with BOTH the dask_executor AND schema extraction.

This demonstrates that you can:
1. Use `dask_executor` to distribute asset materializations across Dask workers
2. Within each asset, use Dask DataFrames and extract schema metadata

The key insight: These are independent concerns that compose well together.
"""

import dask.dataframe as dd
import numpy as np
import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, asset

from dagster_dask_lineage.lineage import (
    ColumnLineageTracker,
    extract_schema_from_dask,
    extract_schema_from_pandas,
)


@asset(
    group_name="combined_example",
    description="Source data - works with dask_executor and extracts schema",
    compute_kind="dask",
)
def source_data(context: AssetExecutionContext) -> MaterializeResult:
    """Generate source data using Dask, extract schema.

    When run with dask_executor:
    - This entire asset runs on a Dask worker
    - We still use Dask DataFrames internally for parallel processing
    - Schema extraction works the same way

    No DaskResource needed here - we create a local Dask DataFrame directly.
    The dask_executor handles the distribution of THIS asset to a worker.
    """
    np.random.seed(42)
    n_rows = 50000

    # Create data as pandas, convert to Dask for parallel processing
    pdf = pd.DataFrame({
        "id": range(n_rows),
        "value": np.random.randn(n_rows),
        "category": np.random.choice(["A", "B", "C"], n_rows),
        "timestamp": pd.date_range("2024-01-01", periods=n_rows, freq="min"),
    })

    # Use Dask for parallel processing within this asset
    ddf = dd.from_pandas(pdf, npartitions=4)

    # Extract schema from Dask DataFrame's _meta - works regardless of executor!
    schema = extract_schema_from_dask(ddf)
    context.log.info(f"Schema extracted: {[c.name for c in schema.columns]}")

    # Compute result
    result = ddf.compute()

    return MaterializeResult(
        metadata={
            "dagster/column_schema": schema,
            "dagster/row_count": len(result),
            "npartitions_used": ddf.npartitions,
        }
    )


@asset(
    group_name="combined_example",
    description="Transformed data with column lineage",
    compute_kind="dask",
    deps=[source_data],
)
def transformed_data(context: AssetExecutionContext) -> MaterializeResult:
    """Transform data with explicit column lineage tracking.

    This asset:
    - Runs on a Dask worker when using dask_executor
    - Uses Dask DataFrames for parallel computation
    - Extracts schema automatically
    - Tracks column lineage explicitly
    """
    # Regenerate source data (in production, you'd read from storage)
    np.random.seed(42)
    n_rows = 50000

    pdf = pd.DataFrame({
        "id": range(n_rows),
        "value": np.random.randn(n_rows),
        "category": np.random.choice(["A", "B", "C"], n_rows),
        "timestamp": pd.date_range("2024-01-01", periods=n_rows, freq="min"),
    })

    ddf = dd.from_pandas(pdf, npartitions=4)

    # Transform with Dask
    ddf["value_squared"] = ddf["value"] ** 2
    ddf["value_abs"] = ddf["value"].abs()
    ddf["hour"] = ddf["timestamp"].dt.hour

    # Track column lineage explicitly
    tracker = ColumnLineageTracker()
    tracker.add_passthrough_columns(["id", "value", "category", "timestamp"], "source_data")
    tracker.add_column_dep("value_squared", "source_data", ["value"])
    tracker.add_column_dep("value_abs", "source_data", ["value"])
    tracker.add_column_dep("hour", "source_data", ["timestamp"])

    # Extract schema and compute
    schema = extract_schema_from_dask(ddf)
    result = ddf.compute()

    return MaterializeResult(
        metadata={
            "dagster/column_schema": schema,
            "dagster/row_count": len(result),
            "dagster/column_lineage": tracker.get_table_column_lineage(),
        }
    )


@asset(
    group_name="combined_example",
    description="Aggregated metrics by category",
    compute_kind="dask",
    deps=[transformed_data],
)
def category_metrics(context: AssetExecutionContext) -> MaterializeResult:
    """Compute category-level metrics with lineage."""
    np.random.seed(42)
    n_rows = 50000

    # Simulate reading transformed data
    pdf = pd.DataFrame({
        "id": range(n_rows),
        "value": np.random.randn(n_rows),
        "category": np.random.choice(["A", "B", "C"], n_rows),
        "value_squared": np.random.randn(n_rows) ** 2,
    })

    ddf = dd.from_pandas(pdf, npartitions=4)

    # Aggregate with Dask
    agg_result = ddf.groupby("category").agg({
        "id": "count",
        "value": ["mean", "std"],
        "value_squared": "sum",
    })

    # Flatten columns
    agg_result.columns = ["count", "value_mean", "value_std", "value_squared_sum"]
    agg_result = agg_result.reset_index()

    # Track lineage from aggregations
    tracker = ColumnLineageTracker()
    tracker.add_column_dep("category", "transformed_data", ["category"])
    tracker.add_column_dep("count", "transformed_data", ["id"])
    tracker.add_column_dep("value_mean", "transformed_data", ["value"])
    tracker.add_column_dep("value_std", "transformed_data", ["value"])
    tracker.add_column_dep("value_squared_sum", "transformed_data", ["value_squared"])

    schema = extract_schema_from_dask(agg_result)
    result = agg_result.compute()

    return MaterializeResult(
        metadata={
            "dagster/column_schema": schema,
            "dagster/row_count": len(result),
            "dagster/column_lineage": tracker.get_table_column_lineage(),
            "preview": result.to_markdown(),
        }
    )
