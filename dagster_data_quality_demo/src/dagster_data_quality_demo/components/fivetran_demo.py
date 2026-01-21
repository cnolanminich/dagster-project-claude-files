"""Fivetran Demo Component for Dagster.

This component simulates Fivetran connector syncs for demo purposes.
It supports both real Fivetran API calls and a demo mode for local testing.
"""

from collections.abc import Sequence
from dataclasses import dataclass

import dagster as dg
from dagster.components import (
    Component,
    ComponentLoadContext,
    Resolvable,
)
from pydantic import BaseModel


class FivetranConnector(BaseModel):
    """Configuration for a Fivetran connector."""

    connector_id: str
    name: str
    destination_schema: str
    sync_tables: Sequence[str]


@dataclass
class FivetranDemoComponent(Component, Resolvable):
    """Component that simulates Fivetran connector syncs.

    This component creates assets representing Fivetran connector destinations.
    In demo mode, it simulates the sync process without requiring Fivetran API access.

    Attributes:
        api_key_env_var: Environment variable for Fivetran API key
        api_secret_env_var: Environment variable for Fivetran API secret
        connectors: List of Fivetran connectors to sync
        demo_mode: If True, simulates Fivetran syncs with mock data
    """

    api_key_env_var: str
    api_secret_env_var: str
    connectors: Sequence[FivetranConnector]
    demo_mode: bool = True

    @classmethod
    def get_component_name(cls) -> str:
        return "fivetran_demo"

    @classmethod
    def get_description(cls) -> str:
        return "Simulates Fivetran connector syncs for demo purposes"

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        """Build Dagster definitions for all connectors."""

        assets = []

        for connector in self.connectors:
            for table in connector.sync_tables:
                asset = self._create_table_asset(connector, table)
                assets.append(asset)

        return dg.Definitions(assets=assets)

    def _create_table_asset(
        self, connector: FivetranConnector, table: str
    ) -> dg.AssetsDefinition:
        """Create an asset for a single Fivetran table sync."""

        demo_mode = self.demo_mode
        api_key_env_var = self.api_key_env_var
        api_secret_env_var = self.api_secret_env_var

        @dg.asset(
            key=dg.AssetKey([connector.destination_schema, table]),
            description=f"Fivetran sync: {connector.name} -> {connector.destination_schema}.{table}",
            metadata={
                "connector_id": connector.connector_id,
                "connector_name": connector.name,
                "destination_schema": connector.destination_schema,
                "demo_mode": demo_mode,
            },
            kinds={"fivetran", "snowflake"},
        )
        def fivetran_sync(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            """Execute Fivetran sync for this table."""

            if demo_mode:
                return _execute_demo_sync(context, connector, table)
            else:
                return _execute_production_sync(
                    context, connector, table, api_key_env_var, api_secret_env_var
                )

        # Rename the asset function to avoid conflicts
        fivetran_sync.__name__ = f"fivetran_{connector.destination_schema}_{table}"

        return fivetran_sync


def _execute_demo_sync(
    context: dg.AssetExecutionContext,
    connector: FivetranConnector,
    table: str,
) -> dg.MaterializeResult:
    """Execute demo mode sync with simulated results."""
    import random
    from datetime import datetime, timedelta

    context.log.info(
        f"DEMO MODE: Simulating Fivetran sync for {connector.name} -> {table}"
    )

    # Simulate sync metrics
    rows_synced = random.randint(1000, 50000)
    sync_duration_seconds = random.uniform(5, 60)
    last_sync = datetime.now() - timedelta(minutes=random.randint(1, 60))

    context.log.info(f"Simulated sync completed: {rows_synced} rows in {sync_duration_seconds:.1f}s")

    return dg.MaterializeResult(
        metadata={
            "demo_mode": True,
            "connector_id": connector.connector_id,
            "connector_name": connector.name,
            "table": table,
            "destination": f"{connector.destination_schema}.{table}",
            "rows_synced": rows_synced,
            "sync_duration_seconds": round(sync_duration_seconds, 2),
            "last_sync_at": last_sync.isoformat(),
            "sync_status": "succeeded",
        }
    )


def _execute_production_sync(
    context: dg.AssetExecutionContext,
    connector: FivetranConnector,
    table: str,
    api_key_env_var: str,
    api_secret_env_var: str,
) -> dg.MaterializeResult:
    """Execute real Fivetran sync via API."""
    import os

    import requests

    context.log.info(f"Triggering Fivetran sync for connector: {connector.connector_id}")

    api_key = os.environ.get(api_key_env_var)
    api_secret = os.environ.get(api_secret_env_var)

    if not api_key or not api_secret:
        raise ValueError(
            f"Fivetran credentials not found. Check environment variables: "
            f"{api_key_env_var}, {api_secret_env_var}"
        )

    # Trigger sync
    url = f"https://api.fivetran.com/v1/connectors/{connector.connector_id}/force"
    response = requests.post(
        url,
        auth=(api_key, api_secret),
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    response.raise_for_status()

    result = response.json()

    context.log.info(f"Fivetran sync triggered: {result}")

    return dg.MaterializeResult(
        metadata={
            "demo_mode": False,
            "connector_id": connector.connector_id,
            "connector_name": connector.name,
            "table": table,
            "destination": f"{connector.destination_schema}.{table}",
            "sync_response": result,
        }
    )
