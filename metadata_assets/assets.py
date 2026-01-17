"""Assets with metadata for demonstrating metadata passing between assets."""

import random
from datetime import datetime

from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    asset,
)


@asset
def asset_a(context: AssetExecutionContext) -> MaterializeResult:
    """Asset A generates random metadata values that can be retrieved by Asset B."""
    random_value = random.randint(1, 100)
    timestamp = datetime.now().isoformat()
    run_id = context.run_id

    context.log.info(f"Asset A generating metadata: value={random_value}, timestamp={timestamp}")

    return MaterializeResult(
        metadata={
            "random_value": random_value,
            "timestamp": timestamp,
            "run_id": run_id,
            "source": "asset_a",
        }
    )


@asset
def asset_b(context: AssetExecutionContext) -> MaterializeResult:
    """Asset B retrieves metadata from historical Asset A runs and uses it."""

    # Fetch the most recent materializations from Asset A
    result = context.instance.fetch_materializations(
        records_filter=AssetKey(["asset_a"]),
        limit=10,
    )

    if not result.records:
        context.log.warning("No previous Asset A materializations found")
        return MaterializeResult(
            metadata={
                "status": "no_previous_data",
                "message": "No Asset A materializations found",
            }
        )

    # Extract metadata from all fetched records
    collected_values = []
    collected_timestamps = []
    collected_run_ids = []

    for record in result.records:
        dagster_event = record.event_log_entry.dagster_event
        if dagster_event is not None:
            materialization = dagster_event.event_specific_data.materialization
            metadata = {key: value.value for key, value in materialization.metadata.items()}

            if "random_value" in metadata:
                collected_values.append(metadata["random_value"])
            if "timestamp" in metadata:
                collected_timestamps.append(metadata["timestamp"])
            if "run_id" in metadata:
                collected_run_ids.append(metadata["run_id"])

    # Compute aggregations from Asset A's metadata
    total_runs_found = len(collected_values)
    avg_value = sum(collected_values) / len(collected_values) if collected_values else 0
    min_value = min(collected_values) if collected_values else 0
    max_value = max(collected_values) if collected_values else 0

    context.log.info(f"Asset B processed {total_runs_found} Asset A runs")
    context.log.info(f"Values from Asset A: {collected_values}")
    context.log.info(f"Average value: {avg_value}, Min: {min_value}, Max: {max_value}")

    return MaterializeResult(
        metadata={
            "status": "success",
            "total_asset_a_runs_processed": total_runs_found,
            "collected_values_from_asset_a": collected_values,
            "average_value": round(avg_value, 2),
            "min_value": min_value,
            "max_value": max_value,
            "first_timestamp": collected_timestamps[0] if collected_timestamps else None,
            "last_timestamp": collected_timestamps[-1] if collected_timestamps else None,
            "processed_run_ids": collected_run_ids,
        }
    )
