"""Jobs for running assets."""

from dagster import define_asset_job

from .assets import asset_a, asset_b


# Job to run Asset A for specific partitions
asset_a_job = define_asset_job(
    name="asset_a_job",
    selection=[asset_a],
    description="Job to materialize Asset A for specific partitions",
)

# Job to run Asset B for specific partitions
asset_b_job = define_asset_job(
    name="asset_b_job",
    selection=[asset_b],
    description="Job to materialize Asset B for specific partitions",
)
