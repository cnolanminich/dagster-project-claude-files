"""Dagster definitions for the metadata assets project."""

from dagster import Definitions

from .assets import asset_a, asset_b
from .jobs import asset_a_job, asset_b_job, metadata_transfer_job


defs = Definitions(
    assets=[asset_a, asset_b],
    jobs=[asset_a_job, asset_b_job, metadata_transfer_job],
)
