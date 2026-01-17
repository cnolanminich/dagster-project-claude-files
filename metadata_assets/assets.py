"""Assets with metadata for demonstrating metadata migration between assets."""

import random
from datetime import datetime

from dagster import (
    AssetExecutionContext,
    AssetKey,
    DailyPartitionsDefinition,
    MaterializeResult,
    asset,
)


# Daily partitions starting from a recent date
daily_partitions = DailyPartitionsDefinition(start_date="2025-01-01")


@asset(partitions_def=daily_partitions)
def asset_a(context: AssetExecutionContext) -> MaterializeResult:
    """Asset A generates random metadata values for each partition."""
    partition_key = context.partition_key
    random_value = random.randint(1, 100)
    timestamp = datetime.now().isoformat()
    run_id = context.run_id

    context.log.info(
        f"Asset A generating metadata for partition {partition_key}: "
        f"value={random_value}, timestamp={timestamp}"
    )

    return MaterializeResult(
        metadata={
            "random_value": random_value,
            "timestamp": timestamp,
            "run_id": run_id,
            "source": "asset_a",
            "partition": partition_key,
        }
    )


@asset(partitions_def=daily_partitions)
def asset_b(context: AssetExecutionContext) -> MaterializeResult:
    """Asset B - target asset for metadata migration. Can also be run independently."""
    partition_key = context.partition_key
    timestamp = datetime.now().isoformat()

    context.log.info(f"Asset B materializing for partition {partition_key}")

    return MaterializeResult(
        metadata={
            "timestamp": timestamp,
            "run_id": context.run_id,
            "source": "asset_b_direct",
            "partition": partition_key,
        }
    )
