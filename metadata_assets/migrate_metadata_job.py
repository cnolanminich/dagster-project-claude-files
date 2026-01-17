"""Factory pattern for migrating materialization events between assets in bulk."""

from typing import List, Optional, Tuple

from dagster import (
    AssetKey,
    AssetMaterialization,
    Config,
    DynamicOut,
    DynamicOutput,
    In,
    Nothing,
    OpExecutionContext,
    Out,
    graph,
    job,
    op,
)


class AssetMigrationMapping(Config):
    """Configuration for a single asset migration mapping."""
    source_asset_key: str
    target_asset_key: str


class BulkMigrationConfig(Config):
    """Configuration for bulk metadata migration."""
    migrations: List[AssetMigrationMapping]
    limit_per_asset: Optional[int] = None  # None means migrate all
    batch_size: int = 100


class SingleMigrationConfig(Config):
    """Configuration for a single asset migration."""
    source_asset_key: str
    target_asset_key: str
    limit: Optional[int] = None
    batch_size: int = 100


def migrate_asset_metadata(
    context: OpExecutionContext,
    source_key: AssetKey,
    target_key: AssetKey,
    limit: Optional[int] = None,
    batch_size: int = 100,
) -> dict:
    """
    Core migration function that copies materialization events from source to target asset.

    Args:
        context: Dagster op execution context
        source_key: Source asset key to migrate from
        target_key: Target asset key to migrate to
        limit: Maximum number of events to migrate (None for all)
        batch_size: Number of records to fetch per batch

    Returns:
        Dictionary with migration statistics
    """
    context.log.info(f"Starting migration from {source_key} to {target_key}")

    cursor = None
    total_migrated = 0
    errors = []

    while True:
        # Calculate batch limit
        if limit is None:
            current_batch_size = batch_size
        else:
            remaining = limit - total_migrated
            if remaining <= 0:
                break
            current_batch_size = min(batch_size, remaining)

        # Fetch a batch of materializations
        result = context.instance.fetch_materializations(
            records_filter=source_key,
            limit=current_batch_size,
            cursor=cursor,
        )

        if not result.records:
            context.log.info(f"No more records to migrate for {source_key}")
            break

        context.log.info(f"Processing batch of {len(result.records)} records for {source_key}")

        for record in result.records:
            try:
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

                context.log.debug(
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

            except Exception as e:
                error_msg = f"Error migrating record {record.storage_id}: {str(e)}"
                context.log.error(error_msg)
                errors.append(error_msg)

        # Update cursor for next batch
        cursor = result.cursor

        # If we got fewer records than batch_size, we've reached the end
        if len(result.records) < current_batch_size:
            break

    context.log.info(
        f"Migration complete for {source_key} -> {target_key}. "
        f"Total events migrated: {total_migrated}"
    )

    return {
        "source": str(source_key),
        "target": str(target_key),
        "total_migrated": total_migrated,
        "errors": errors,
    }


# ============================================================================
# Single Migration Op and Job
# ============================================================================

@op(out=Out(dict))
def migrate_single_asset(context: OpExecutionContext, config: SingleMigrationConfig) -> dict:
    """Op to migrate a single asset's metadata to another asset."""
    source_key = AssetKey.from_user_string(config.source_asset_key)
    target_key = AssetKey.from_user_string(config.target_asset_key)

    return migrate_asset_metadata(
        context=context,
        source_key=source_key,
        target_key=target_key,
        limit=config.limit,
        batch_size=config.batch_size,
    )


@job
def migrate_metadata_job():
    """Job to migrate materialization events from one asset to another."""
    migrate_single_asset()


# ============================================================================
# Bulk Migration Ops and Job
# ============================================================================

@op(out=DynamicOut(dict))
def generate_migration_tasks(context: OpExecutionContext, config: BulkMigrationConfig):
    """Generate dynamic outputs for each migration mapping."""
    context.log.info(f"Generating {len(config.migrations)} migration tasks")

    for i, mapping in enumerate(config.migrations):
        task_id = f"{mapping.source_asset_key}_to_{mapping.target_asset_key}".replace("/", "_")
        context.log.info(f"Creating task: {mapping.source_asset_key} -> {mapping.target_asset_key}")

        yield DynamicOutput(
            value={
                "source_asset_key": mapping.source_asset_key,
                "target_asset_key": mapping.target_asset_key,
                "limit": config.limit_per_asset,
                "batch_size": config.batch_size,
            },
            mapping_key=task_id,
        )


@op(out=Out(dict))
def execute_migration(context: OpExecutionContext, migration_config: dict) -> dict:
    """Execute a single migration from the bulk configuration."""
    source_key = AssetKey.from_user_string(migration_config["source_asset_key"])
    target_key = AssetKey.from_user_string(migration_config["target_asset_key"])

    return migrate_asset_metadata(
        context=context,
        source_key=source_key,
        target_key=target_key,
        limit=migration_config.get("limit"),
        batch_size=migration_config.get("batch_size", 100),
    )


@op(ins={"results": In(List[dict])}, out=Out(dict))
def aggregate_results(context: OpExecutionContext, results: List[dict]) -> dict:
    """Aggregate results from all migrations."""
    total_migrated = sum(r["total_migrated"] for r in results)
    all_errors = []

    for r in results:
        if r.get("errors"):
            all_errors.extend(r["errors"])

    summary = {
        "total_assets_migrated": len(results),
        "total_events_migrated": total_migrated,
        "migrations": results,
        "total_errors": len(all_errors),
        "errors": all_errors,
    }

    context.log.info(
        f"Bulk migration complete. "
        f"Assets: {len(results)}, Events: {total_migrated}, Errors: {len(all_errors)}"
    )

    return summary


@graph
def bulk_migration_graph():
    """Graph for bulk migration of multiple assets."""
    tasks = generate_migration_tasks()
    results = tasks.map(execute_migration)
    return aggregate_results(results.collect())


bulk_migrate_metadata_job = bulk_migration_graph.to_job(name="bulk_migrate_metadata_job")


# ============================================================================
# Factory Functions for Custom Jobs
# ============================================================================

def create_migration_job(
    name: str,
    source_asset_key: str,
    target_asset_key: str,
    description: Optional[str] = None,
):
    """
    Factory function to create a migration job for a specific asset pair.

    Args:
        name: Name of the job
        source_asset_key: Source asset key string
        target_asset_key: Target asset key string
        description: Optional job description

    Returns:
        A configured Dagster job

    Example:
        my_migration_job = create_migration_job(
            name="migrate_orders_to_archive",
            source_asset_key="orders",
            target_asset_key="orders_archive",
        )
    """
    @op(name=f"migrate_{name}")
    def _migrate_op(context: OpExecutionContext, config: SingleMigrationConfig) -> dict:
        source_key = AssetKey.from_user_string(config.source_asset_key)
        target_key = AssetKey.from_user_string(config.target_asset_key)
        return migrate_asset_metadata(
            context=context,
            source_key=source_key,
            target_key=target_key,
            limit=config.limit,
            batch_size=config.batch_size,
        )

    @graph(name=f"{name}_graph")
    def _migration_graph():
        return _migrate_op()

    return _migration_graph.to_job(
        name=name,
        description=description or f"Migrate metadata from {source_asset_key} to {target_asset_key}",
    )


def create_bulk_migration_job(
    name: str,
    migrations: List[Tuple[str, str]],
    description: Optional[str] = None,
):
    """
    Factory function to create a bulk migration job for multiple asset pairs.

    Args:
        name: Name of the job
        migrations: List of (source_asset_key, target_asset_key) tuples
        description: Optional job description

    Returns:
        A configured Dagster job

    Example:
        my_bulk_job = create_bulk_migration_job(
            name="migrate_all_to_archive",
            migrations=[
                ("orders", "orders_archive"),
                ("customers", "customers_archive"),
                ("products", "products_archive"),
            ],
        )
    """
    migration_configs = [
        {"source": src, "target": tgt} for src, tgt in migrations
    ]

    @op(name=f"migrate_bulk_{name}", out=DynamicOut(dict))
    def _generate_tasks(context: OpExecutionContext, config: BulkMigrationConfig):
        # Use predefined migrations if config is empty, otherwise use config
        effective_migrations = config.migrations if config.migrations else [
            AssetMigrationMapping(source_asset_key=m["source"], target_asset_key=m["target"])
            for m in migration_configs
        ]

        context.log.info(f"Generating {len(effective_migrations)} migration tasks")

        for mapping in effective_migrations:
            task_id = f"{mapping.source_asset_key}_to_{mapping.target_asset_key}".replace("/", "_")
            yield DynamicOutput(
                value={
                    "source_asset_key": mapping.source_asset_key,
                    "target_asset_key": mapping.target_asset_key,
                    "limit": config.limit_per_asset,
                    "batch_size": config.batch_size,
                },
                mapping_key=task_id,
            )

    @op(name=f"execute_{name}")
    def _execute_migration(context: OpExecutionContext, migration_config: dict) -> dict:
        source_key = AssetKey.from_user_string(migration_config["source_asset_key"])
        target_key = AssetKey.from_user_string(migration_config["target_asset_key"])
        return migrate_asset_metadata(
            context=context,
            source_key=source_key,
            target_key=target_key,
            limit=migration_config.get("limit"),
            batch_size=migration_config.get("batch_size", 100),
        )

    @op(name=f"summarize_{name}", ins={"results": In(List[dict])})
    def _aggregate(context: OpExecutionContext, results: List[dict]) -> dict:
        total = sum(r["total_migrated"] for r in results)
        context.log.info(f"Migrated {total} total events across {len(results)} assets")
        return {"total_assets": len(results), "total_events": total, "details": results}

    @graph(name=f"{name}_graph")
    def _bulk_graph():
        tasks = _generate_tasks()
        results = tasks.map(_execute_migration)
        return _aggregate(results.collect())

    return _bulk_graph.to_job(
        name=name,
        description=description or f"Bulk migrate metadata for {len(migrations)} asset pairs",
    )


# ============================================================================
# Pre-configured Example Jobs
# ============================================================================

# Example: Create a specific migration job using the factory
migrate_a_to_b_job = create_migration_job(
    name="migrate_a_to_b",
    source_asset_key="asset_a",
    target_asset_key="asset_b",
    description="Migrate all materialization events from asset_a to asset_b",
)
