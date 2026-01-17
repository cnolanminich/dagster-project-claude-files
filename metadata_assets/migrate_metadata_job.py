"""Job to migrate materialization events from Asset A to Asset B."""

from typing import Optional

from dagster import (
    AssetKey,
    AssetMaterialization,
    Config,
    OpExecutionContext,
    job,
    op,
)


class MigrationConfig(Config):
    """Configuration for the metadata migration job."""
    source_asset_key: str = "asset_a"
    target_asset_key: str = "asset_b"
    limit: Optional[int] = None  # None means migrate all


@op
def migrate_metadata(context: OpExecutionContext, config: MigrationConfig):
    """
    Migrate all materialization events from source asset to target asset.

    This op:
    1. Fetches all materialization records from the source asset
    2. For each record, reports an AssetMaterialization event for the target asset
       with the same metadata and partition information
    """
    source_key = AssetKey.from_user_string(config.source_asset_key)
    target_key = AssetKey.from_user_string(config.target_asset_key)

    context.log.info(f"Starting migration from {source_key} to {target_key}")

    # Fetch all materializations from source asset
    cursor = None
    total_migrated = 0
    batch_size = 100

    while True:
        # Fetch a batch of materializations
        result = context.instance.fetch_materializations(
            records_filter=source_key,
            limit=batch_size if config.limit is None else min(batch_size, config.limit - total_migrated),
            cursor=cursor,
        )

        if not result.records:
            context.log.info("No more records to migrate")
            break

        context.log.info(f"Processing batch of {len(result.records)} records")

        for record in result.records:
            dagster_event = record.event_log_entry.dagster_event
            if dagster_event is None:
                continue

            materialization = dagster_event.event_specific_data.materialization

            # Extract metadata from source materialization
            metadata = {key: value.value for key, value in materialization.metadata.items()}

            # Add migration tracking metadata
            metadata["migrated_from"] = str(source_key)
            metadata["original_storage_id"] = record.storage_id

            # Get partition information
            partition = materialization.partition

            context.log.info(
                f"Migrating event: partition={partition}, "
                f"metadata_keys={list(metadata.keys())}"
            )

            # Report the materialization for the target asset
            context.instance.report_runless_asset_event(
                AssetMaterialization(
                    asset_key=target_key,
                    partition=partition,
                    metadata=metadata,
                )
            )

            total_migrated += 1

            # Check if we've hit the limit
            if config.limit is not None and total_migrated >= config.limit:
                context.log.info(f"Reached migration limit of {config.limit}")
                break

        # Update cursor for next batch
        cursor = result.cursor

        # Check if we've hit the limit
        if config.limit is not None and total_migrated >= config.limit:
            break

        # If we got fewer records than batch_size, we've reached the end
        if len(result.records) < batch_size:
            break

    context.log.info(f"Migration complete. Total events migrated: {total_migrated}")

    return {"total_migrated": total_migrated}


@job
def migrate_metadata_job():
    """Job to migrate materialization events from Asset A to Asset B."""
    migrate_metadata()
