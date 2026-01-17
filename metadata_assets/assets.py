"""Assets with metadata for demonstrating metadata migration between assets."""

import random
from datetime import datetime

from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MaterializeResult,
    asset,
)


# Daily partitions starting from a recent date
daily_partitions = DailyPartitionsDefinition(start_date="2025-01-01")


def _generate_asset_metadata(
    context: AssetExecutionContext,
    asset_name: str,
) -> MaterializeResult:
    """Helper to generate consistent metadata for assets."""
    partition_key = context.partition_key
    random_value = random.randint(1, 100)
    timestamp = datetime.now().isoformat()

    context.log.info(
        f"{asset_name} generating metadata for partition {partition_key}: "
        f"value={random_value}, timestamp={timestamp}"
    )

    return MaterializeResult(
        metadata={
            "random_value": random_value,
            "timestamp": timestamp,
            "run_id": context.run_id,
            "source": asset_name,
            "partition": partition_key,
        }
    )


@asset(partitions_def=daily_partitions)
def asset_a(context: AssetExecutionContext) -> MaterializeResult:
    """Asset A generates random metadata values for each partition."""
    return _generate_asset_metadata(context, "asset_a")


@asset(partitions_def=daily_partitions)
def asset_b(context: AssetExecutionContext) -> MaterializeResult:
    """Asset B - target asset for metadata migration from Asset A."""
    return _generate_asset_metadata(context, "asset_b")


@asset(partitions_def=daily_partitions)
def asset_c(context: AssetExecutionContext) -> MaterializeResult:
    """Asset C generates random metadata values for each partition."""
    return _generate_asset_metadata(context, "asset_c")


@asset(partitions_def=daily_partitions)
def asset_d(context: AssetExecutionContext) -> MaterializeResult:
    """Asset D - target asset for metadata migration from Asset C."""
    return _generate_asset_metadata(context, "asset_d")


# Additional assets for bulk migration demo
@asset(partitions_def=daily_partitions)
def orders(context: AssetExecutionContext) -> MaterializeResult:
    """Orders asset with sales data metadata."""
    partition_key = context.partition_key
    order_count = random.randint(100, 1000)
    revenue = round(random.uniform(1000, 50000), 2)

    context.log.info(f"Orders for {partition_key}: count={order_count}, revenue=${revenue}")

    return MaterializeResult(
        metadata={
            "order_count": order_count,
            "revenue": revenue,
            "timestamp": datetime.now().isoformat(),
            "partition": partition_key,
        }
    )


@asset(partitions_def=daily_partitions)
def orders_archive(context: AssetExecutionContext) -> MaterializeResult:
    """Archived orders - target for orders migration."""
    return _generate_asset_metadata(context, "orders_archive")


@asset(partitions_def=daily_partitions)
def customers(context: AssetExecutionContext) -> MaterializeResult:
    """Customers asset with customer metrics."""
    partition_key = context.partition_key
    new_customers = random.randint(10, 100)
    active_customers = random.randint(500, 5000)

    context.log.info(f"Customers for {partition_key}: new={new_customers}, active={active_customers}")

    return MaterializeResult(
        metadata={
            "new_customers": new_customers,
            "active_customers": active_customers,
            "timestamp": datetime.now().isoformat(),
            "partition": partition_key,
        }
    )


@asset(partitions_def=daily_partitions)
def customers_archive(context: AssetExecutionContext) -> MaterializeResult:
    """Archived customers - target for customers migration."""
    return _generate_asset_metadata(context, "customers_archive")
