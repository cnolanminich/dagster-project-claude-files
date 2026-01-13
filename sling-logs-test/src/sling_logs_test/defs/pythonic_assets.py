"""Pythonic approach to Sling assets using @sling_assets decorator.

This module demonstrates the traditional Python-based way to define sling assets
using the @sling_assets decorator with explicit resource handling.
"""
from pathlib import Path

from dagster import Definitions
from dagster_sling import SlingConnectionResource, SlingResource, sling_assets


# Path to replication config - relative to this module
REPLICATION_CONFIG = Path(__file__).parent / "pythonic_replication.yaml"

# Define the sling resource with connections
sling_resource = SlingResource(
    connections=[
        SlingConnectionResource(name="LOCAL_FILE", type="file"),
    ]
)


@sling_assets(replication_config=REPLICATION_CONFIG, name="pythonic_sling_assets")
def pythonic_sling_asset(context, sling: SlingResource):
    """Execute the sling replication using the Pythonic decorator approach.

    This function is called by Dagster when materializing the asset.
    The sling resource is injected by Dagster.
    """
    yield from sling.replicate(context=context)


# Export the Definitions for load_from_defs_folder to discover
defs = Definitions(
    assets=[pythonic_sling_asset],
    resources={"sling": sling_resource},
)
