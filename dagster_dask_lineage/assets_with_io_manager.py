"""Assets demonstrating AUTOMATIC schema extraction via IOManager.

When using the DaskParquetIOManager, you don't need to manually extract
or attach schema/row count metadata - it's done automatically!

This is the answer to "how to automatically tell all Dagster assets
to get schema and row count" - use an IOManager that handles it.
"""

import dask.dataframe as dd
import numpy as np
import pandas as pd
from dagster import asset


# =============================================================================
# SIMPLE ASSETS - No manual metadata needed!
# =============================================================================
# When using DaskParquetIOManager, these assets will AUTOMATICALLY get:
# - dagster/column_schema
# - dagster/row_count
# - npartitions


@asset(group_name="auto_metadata")
def auto_orders() -> dd.DataFrame:
    """Orders data - schema and row count extracted automatically!

    Notice: No MaterializeResult, no manual metadata extraction.
    The IOManager handles everything.
    """
    np.random.seed(42)
    n_rows = 10000

    pdf = pd.DataFrame({
        "order_id": range(1, n_rows + 1),
        "customer_id": np.random.randint(1, 1001, n_rows),
        "order_date": pd.date_range("2024-01-01", periods=n_rows, freq="h"),
        "amount": np.round(np.random.uniform(10, 500, n_rows), 2),
        "status": np.random.choice(["pending", "completed", "cancelled"], n_rows),
    })

    return dd.from_pandas(pdf, npartitions=4)


@asset(group_name="auto_metadata")
def auto_customers() -> dd.DataFrame:
    """Customers data - schema and row count extracted automatically!"""
    np.random.seed(123)
    n_customers = 1000

    pdf = pd.DataFrame({
        "customer_id": range(1, n_customers + 1),
        "name": [f"Customer {i}" for i in range(1, n_customers + 1)],
        "email": [f"customer{i}@example.com" for i in range(1, n_customers + 1)],
        "country": np.random.choice(["US", "UK", "CA", "DE"], n_customers),
        "signup_date": pd.date_range("2020-01-01", periods=n_customers, freq="D"),
    })

    return dd.from_pandas(pdf, npartitions=2)


@asset(group_name="auto_metadata", deps=[auto_orders, auto_customers])
def auto_customer_summary(auto_orders: dd.DataFrame, auto_customers: dd.DataFrame) -> dd.DataFrame:
    """Customer summary - joins orders and customers.

    The IOManager:
    1. Loads auto_orders and auto_customers as Dask DataFrames
    2. Stores the result and automatically extracts schema/row count
    """
    # Aggregate orders per customer
    order_summary = auto_orders.groupby("customer_id").agg({
        "order_id": "count",
        "amount": "sum",
    }).reset_index()
    order_summary.columns = ["customer_id", "order_count", "total_spent"]

    # Join with customers
    result = auto_customers.merge(order_summary, on="customer_id", how="left")
    result["order_count"] = result["order_count"].fillna(0)
    result["total_spent"] = result["total_spent"].fillna(0)

    return result


# =============================================================================
# ALSO WORKS WITH PANDAS!
# =============================================================================
# If you use DaskPandasParquetIOManager, you can return Pandas DataFrames
# and still get automatic metadata extraction.


@asset(group_name="auto_metadata_pandas")
def pandas_products() -> pd.DataFrame:
    """Products data as Pandas - IOManager converts to Dask for storage.

    With DaskPandasParquetIOManager, you can return Pandas DataFrames
    and still get automatic schema/row count extraction.
    """
    np.random.seed(456)
    n_products = 100

    return pd.DataFrame({
        "product_id": range(1, n_products + 1),
        "name": [f"Product {i}" for i in range(1, n_products + 1)],
        "category": np.random.choice(["Electronics", "Clothing", "Food"], n_products),
        "price": np.round(np.random.uniform(5, 200, n_products), 2),
        "in_stock": np.random.choice([True, False], n_products, p=[0.8, 0.2]),
    })
