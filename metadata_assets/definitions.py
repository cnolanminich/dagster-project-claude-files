"""Dagster definitions for the metadata assets project."""

from dagster import Definitions

from .assets import (
    asset_a,
    asset_b,
    asset_c,
    asset_d,
    orders,
    orders_archive,
    customers,
    customers_archive,
)
from .jobs import asset_a_job, asset_b_job
from .migrate_metadata_job import (
    # Single migration jobs
    migrate_metadata_job,
    migrate_a_to_b_job,
    # Bulk migration job
    bulk_migrate_metadata_job,
    # Factory functions for custom jobs
    create_migration_job,
    create_bulk_migration_job,
)


# Create example bulk migration job using the factory
archive_migration_job = create_bulk_migration_job(
    name="archive_migration_job",
    migrations=[
        ("orders", "orders_archive"),
        ("customers", "customers_archive"),
    ],
    description="Migrate orders and customers data to archive assets",
)


defs = Definitions(
    assets=[
        asset_a,
        asset_b,
        asset_c,
        asset_d,
        orders,
        orders_archive,
        customers,
        customers_archive,
    ],
    jobs=[
        # Asset jobs
        asset_a_job,
        asset_b_job,
        # Single migration jobs
        migrate_metadata_job,
        migrate_a_to_b_job,
        # Bulk migration jobs
        bulk_migrate_metadata_job,
        archive_migration_job,
    ],
)
