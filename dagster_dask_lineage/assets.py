"""Dagster assets demonstrating Dask integration with schema and lineage tracking.

This module provides example assets that showcase:
1. Using Dask for parallel DataFrame processing
2. Extracting and surfacing column schema metadata
3. Tracking row counts
4. Implementing explicit column-level lineage tracking

The assets form a simple data pipeline:
    raw_orders -> cleaned_orders -> order_summary
    raw_customers -> cleaned_customers -> customer_metrics
    cleaned_orders + cleaned_customers -> customer_order_analysis
"""

import dask.dataframe as dd
import numpy as np
import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, asset

from dagster_dask_lineage.lineage import (
    ColumnLineageTracker,
    create_materialization_metadata,
    extract_schema_from_dask,
    get_dask_metadata_without_compute,
)
from dagster_dask_lineage.resources import DaskResource


# =============================================================================
# RAW DATA ASSETS
# =============================================================================


@asset(
    group_name="raw",
    description="Raw orders data - simulated source data",
    compute_kind="dask",
)
def raw_orders(context: AssetExecutionContext, dask: DaskResource) -> MaterializeResult:
    """Generate simulated raw orders data using Dask.

    This asset demonstrates:
    - Creating a Dask DataFrame from synthetic data
    - Extracting schema metadata without full computation
    - Computing row counts
    """
    # Generate sample data
    np.random.seed(42)
    n_rows = 10000

    data = {
        "order_id": range(1, n_rows + 1),
        "customer_id": np.random.randint(1, 1001, n_rows),
        "order_date": pd.date_range("2024-01-01", periods=n_rows, freq="h"),
        "product_id": np.random.randint(1, 101, n_rows),
        "quantity": np.random.randint(1, 10, n_rows),
        "unit_price": np.round(np.random.uniform(10, 500, n_rows), 2),
        "status": np.random.choice(["pending", "shipped", "delivered", "cancelled"], n_rows),
    }

    pdf = pd.DataFrame(data)

    # Convert to Dask DataFrame
    with dask.get_client() as client:
        ddf = dd.from_pandas(pdf, npartitions=4)

        # Log metadata available WITHOUT computation
        meta_info = get_dask_metadata_without_compute(ddf)
        context.log.info(f"Dask metadata (no compute): {meta_info}")

        # Extract schema from Dask's _meta attribute
        schema = extract_schema_from_dask(ddf)
        context.log.info(f"Extracted schema: {[c.name for c in schema.columns]}")

        # Compute and persist
        result_pdf = ddf.compute()
        row_count = len(result_pdf)

    return MaterializeResult(
        metadata={
            "dagster/column_schema": schema,
            "dagster/row_count": row_count,
            "npartitions": meta_info["npartitions"],
            "preview": result_pdf.head(5).to_markdown(),
        }
    )


@asset(
    group_name="raw",
    description="Raw customers data - simulated source data",
    compute_kind="dask",
)
def raw_customers(context: AssetExecutionContext, dask: DaskResource) -> MaterializeResult:
    """Generate simulated raw customers data using Dask."""
    np.random.seed(123)
    n_customers = 1000

    data = {
        "customer_id": range(1, n_customers + 1),
        "first_name": [f"First_{i}" for i in range(1, n_customers + 1)],
        "last_name": [f"Last_{i}" for i in range(1, n_customers + 1)],
        "email": [f"customer{i}@example.com" for i in range(1, n_customers + 1)],
        "signup_date": pd.date_range("2020-01-01", periods=n_customers, freq="D"),
        "country": np.random.choice(["US", "UK", "CA", "DE", "FR", "AU"], n_customers),
        "is_active": np.random.choice([True, False], n_customers, p=[0.85, 0.15]),
    }

    pdf = pd.DataFrame(data)

    with dask.get_client() as client:
        ddf = dd.from_pandas(pdf, npartitions=2)
        schema = extract_schema_from_dask(ddf)
        result_pdf = ddf.compute()
        row_count = len(result_pdf)

    return MaterializeResult(
        metadata={
            "dagster/column_schema": schema,
            "dagster/row_count": row_count,
            "preview": result_pdf.head(5).to_markdown(),
        }
    )


# =============================================================================
# CLEANED DATA ASSETS (with column lineage)
# =============================================================================


@asset(
    group_name="cleaned",
    description="Cleaned orders with calculated total amount",
    compute_kind="dask",
    deps=[raw_orders],
)
def cleaned_orders(context: AssetExecutionContext, dask: DaskResource) -> MaterializeResult:
    """Clean and transform orders data.

    This asset demonstrates EXPLICIT column-level lineage tracking.
    Since Dask doesn't provide native column lineage, we manually track
    which output columns depend on which input columns.
    """
    # In a real scenario, you'd read from storage. Here we regenerate.
    np.random.seed(42)
    n_rows = 10000

    data = {
        "order_id": range(1, n_rows + 1),
        "customer_id": np.random.randint(1, 1001, n_rows),
        "order_date": pd.date_range("2024-01-01", periods=n_rows, freq="h"),
        "product_id": np.random.randint(1, 101, n_rows),
        "quantity": np.random.randint(1, 10, n_rows),
        "unit_price": np.round(np.random.uniform(10, 500, n_rows), 2),
        "status": np.random.choice(["pending", "shipped", "delivered", "cancelled"], n_rows),
    }

    pdf = pd.DataFrame(data)

    # Initialize lineage tracker
    tracker = ColumnLineageTracker()

    with dask.get_client() as client:
        ddf = dd.from_pandas(pdf, npartitions=4)

        # Filter out cancelled orders
        ddf = ddf[ddf["status"] != "cancelled"]

        # Calculate total_amount (new column)
        ddf["total_amount"] = ddf["quantity"] * ddf["unit_price"]

        # Record column lineage explicitly
        # Passthrough columns - unchanged from source
        tracker.add_passthrough_columns(
            ["order_id", "customer_id", "order_date", "product_id", "quantity", "unit_price"],
            source_asset="raw_orders",
        )

        # Status is filtered but otherwise unchanged
        tracker.add_column_dep("status", "raw_orders", ["status"])

        # total_amount is derived from quantity and unit_price
        tracker.add_column_dep("total_amount", "raw_orders", ["quantity", "unit_price"])

        # Compute
        result_pdf = ddf.compute()
        row_count = len(result_pdf)

        # Get comprehensive metadata including lineage
        metadata = create_materialization_metadata(
            result_pdf, row_count=row_count, lineage_tracker=tracker
        )

    context.log.info(f"Cleaned orders: {row_count} rows (removed cancelled orders)")
    context.log.info(f"Column lineage tracked for {len(tracker._deps)} columns")

    return MaterializeResult(
        metadata={
            **metadata,
            "filtered_out_count": n_rows - row_count,
            "preview": result_pdf.head(5).to_markdown(),
        }
    )


@asset(
    group_name="cleaned",
    description="Cleaned customers with derived columns",
    compute_kind="dask",
    deps=[raw_customers],
)
def cleaned_customers(context: AssetExecutionContext, dask: DaskResource) -> MaterializeResult:
    """Clean and transform customers data with column lineage tracking."""
    np.random.seed(123)
    n_customers = 1000

    data = {
        "customer_id": range(1, n_customers + 1),
        "first_name": [f"First_{i}" for i in range(1, n_customers + 1)],
        "last_name": [f"Last_{i}" for i in range(1, n_customers + 1)],
        "email": [f"customer{i}@example.com" for i in range(1, n_customers + 1)],
        "signup_date": pd.date_range("2020-01-01", periods=n_customers, freq="D"),
        "country": np.random.choice(["US", "UK", "CA", "DE", "FR", "AU"], n_customers),
        "is_active": np.random.choice([True, False], n_customers, p=[0.85, 0.15]),
    }

    pdf = pd.DataFrame(data)

    tracker = ColumnLineageTracker()

    with dask.get_client() as client:
        ddf = dd.from_pandas(pdf, npartitions=2)

        # Filter to active customers only
        ddf = ddf[ddf["is_active"]]

        # Create full_name column (derived from first_name + last_name)
        ddf["full_name"] = ddf["first_name"] + " " + ddf["last_name"]

        # Calculate days since signup
        ddf["days_since_signup"] = (
            pd.Timestamp.now() - ddf["signup_date"]
        ).dt.days

        # Record lineage
        tracker.add_passthrough_columns(
            ["customer_id", "email", "signup_date", "country", "is_active"],
            source_asset="raw_customers",
        )

        # first_name and last_name contribute to full_name
        tracker.add_column_dep("full_name", "raw_customers", ["first_name", "last_name"])

        # days_since_signup derived from signup_date
        tracker.add_column_dep("days_since_signup", "raw_customers", ["signup_date"])

        result_pdf = ddf.compute()
        row_count = len(result_pdf)

        metadata = create_materialization_metadata(
            result_pdf, row_count=row_count, lineage_tracker=tracker
        )

    return MaterializeResult(
        metadata={
            **metadata,
            "active_customer_count": row_count,
            "preview": result_pdf.head(5).to_markdown(),
        }
    )


# =============================================================================
# AGGREGATED/ANALYSIS ASSETS
# =============================================================================


@asset(
    group_name="analytics",
    description="Order summary statistics",
    compute_kind="dask",
    deps=[cleaned_orders],
)
def order_summary(context: AssetExecutionContext, dask: DaskResource) -> MaterializeResult:
    """Compute order summary statistics.

    Demonstrates aggregation with Dask and lineage tracking for
    aggregate columns.
    """
    # Regenerate cleaned orders data
    np.random.seed(42)
    n_rows = 10000

    data = {
        "order_id": range(1, n_rows + 1),
        "customer_id": np.random.randint(1, 1001, n_rows),
        "order_date": pd.date_range("2024-01-01", periods=n_rows, freq="h"),
        "product_id": np.random.randint(1, 101, n_rows),
        "quantity": np.random.randint(1, 10, n_rows),
        "unit_price": np.round(np.random.uniform(10, 500, n_rows), 2),
        "status": np.random.choice(["pending", "shipped", "delivered", "cancelled"], n_rows),
    }

    pdf = pd.DataFrame(data)
    pdf = pdf[pdf["status"] != "cancelled"]
    pdf["total_amount"] = pdf["quantity"] * pdf["unit_price"]

    tracker = ColumnLineageTracker()

    with dask.get_client() as client:
        ddf = dd.from_pandas(pdf, npartitions=4)

        # Aggregate by product_id
        summary = ddf.groupby("product_id").agg({
            "order_id": "count",
            "quantity": "sum",
            "total_amount": ["sum", "mean"],
        })

        # Flatten column names
        summary.columns = ["order_count", "total_quantity", "total_revenue", "avg_order_value"]
        summary = summary.reset_index()

        # Record lineage - aggregations depend on source columns
        tracker.add_column_dep("product_id", "cleaned_orders", ["product_id"])
        tracker.add_column_dep("order_count", "cleaned_orders", ["order_id"])
        tracker.add_column_dep("total_quantity", "cleaned_orders", ["quantity"])
        tracker.add_column_dep("total_revenue", "cleaned_orders", ["total_amount"])
        tracker.add_column_dep("avg_order_value", "cleaned_orders", ["total_amount"])

        result_pdf = summary.compute()
        row_count = len(result_pdf)

        metadata = create_materialization_metadata(
            result_pdf, row_count=row_count, lineage_tracker=tracker
        )

    return MaterializeResult(
        metadata={
            **metadata,
            "total_revenue": float(result_pdf["total_revenue"].sum()),
            "preview": result_pdf.head(10).to_markdown(),
        }
    )


@asset(
    group_name="analytics",
    description="Customer metrics including order statistics",
    compute_kind="dask",
    deps=[cleaned_orders, cleaned_customers],
)
def customer_order_analysis(
    context: AssetExecutionContext, dask: DaskResource
) -> MaterializeResult:
    """Join customers with orders and compute customer-level metrics.

    This demonstrates:
    - Joining multiple Dask DataFrames
    - Complex column lineage from multiple source assets
    """
    # Regenerate data (in production, read from storage)
    np.random.seed(42)
    n_orders = 10000

    orders_data = {
        "order_id": range(1, n_orders + 1),
        "customer_id": np.random.randint(1, 1001, n_orders),
        "total_amount": np.round(
            np.random.randint(1, 10, n_orders) * np.random.uniform(10, 500, n_orders), 2
        ),
    }
    orders_pdf = pd.DataFrame(orders_data)

    np.random.seed(123)
    n_customers = 1000
    customers_data = {
        "customer_id": range(1, n_customers + 1),
        "full_name": [f"First_{i} Last_{i}" for i in range(1, n_customers + 1)],
        "country": np.random.choice(["US", "UK", "CA", "DE", "FR", "AU"], n_customers),
    }
    customers_pdf = pd.DataFrame(customers_data)

    tracker = ColumnLineageTracker()

    with dask.get_client() as client:
        orders_ddf = dd.from_pandas(orders_pdf, npartitions=4)
        customers_ddf = dd.from_pandas(customers_pdf, npartitions=2)

        # Aggregate orders per customer
        customer_orders = orders_ddf.groupby("customer_id").agg({
            "order_id": "count",
            "total_amount": ["sum", "mean"],
        })
        customer_orders.columns = ["order_count", "total_spent", "avg_order_value"]
        customer_orders = customer_orders.reset_index()

        # Join with customer info
        result = customers_ddf.merge(customer_orders, on="customer_id", how="left")

        # Fill NaN for customers with no orders
        result["order_count"] = result["order_count"].fillna(0)
        result["total_spent"] = result["total_spent"].fillna(0)
        result["avg_order_value"] = result["avg_order_value"].fillna(0)

        # Record lineage from MULTIPLE sources
        # Columns from cleaned_customers
        tracker.add_column_dep("customer_id", "cleaned_customers", ["customer_id"])
        tracker.add_column_dep("full_name", "cleaned_customers", ["full_name"])
        tracker.add_column_dep("country", "cleaned_customers", ["country"])

        # Columns derived from cleaned_orders
        tracker.add_column_dep("order_count", "cleaned_orders", ["order_id"])
        tracker.add_column_dep("total_spent", "cleaned_orders", ["total_amount"])
        tracker.add_column_dep("avg_order_value", "cleaned_orders", ["total_amount"])

        result_pdf = result.compute()
        row_count = len(result_pdf)

        metadata = create_materialization_metadata(
            result_pdf, row_count=row_count, lineage_tracker=tracker
        )

    context.log.info(f"Customer analysis complete: {row_count} customers")
    context.log.info(f"Customers with orders: {(result_pdf['order_count'] > 0).sum()}")

    return MaterializeResult(
        metadata={
            **metadata,
            "customers_with_orders": int((result_pdf["order_count"] > 0).sum()),
            "customers_without_orders": int((result_pdf["order_count"] == 0).sum()),
            "preview": result_pdf.head(10).to_markdown(),
        }
    )
