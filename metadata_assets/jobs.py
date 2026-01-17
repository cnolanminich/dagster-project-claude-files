"""Jobs for running assets."""

from dagster import define_asset_job

from .assets import asset_a, asset_b


# Job to run Asset A - can be run multiple times to generate metadata
asset_a_job = define_asset_job(
    name="asset_a_job",
    selection=[asset_a],
    description="Job to materialize Asset A and generate metadata",
)

# Job to run Asset B - reads metadata from Asset A's historical runs
asset_b_job = define_asset_job(
    name="asset_b_job",
    selection=[asset_b],
    description="Job to materialize Asset B which reads Asset A's metadata",
)

# Job to run both assets - useful for demonstration
metadata_transfer_job = define_asset_job(
    name="metadata_transfer_job",
    selection=[asset_a, asset_b],
    description="Job to run both assets - Asset B will read metadata from previous Asset A runs",
)
