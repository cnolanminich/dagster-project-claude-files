"""Dask resource configuration for Dagster.

This module provides a configurable Dask resource that can be used across
multiple assets in a Dagster pipeline.
"""

from contextlib import contextmanager
from typing import Iterator, Literal, Optional

from dagster import ConfigurableResource
from dask.distributed import Client, LocalCluster
from pydantic import Field


class DaskResource(ConfigurableResource):
    """A configurable Dask resource for Dagster.

    This resource manages a Dask client connection that can be shared across
    multiple assets. It supports both local clusters and connections to
    existing Dask schedulers.

    Attributes:
        cluster_type: Type of Dask cluster to use ('local' or 'existing')
        scheduler_address: Address of existing Dask scheduler (only used if cluster_type='existing')
        n_workers: Number of workers for local cluster
        threads_per_worker: Threads per worker for local cluster
        memory_limit: Memory limit per worker (e.g., '2GB')
    """

    cluster_type: Literal["local", "existing"] = Field(
        default="local",
        description="Type of Dask cluster: 'local' creates a new local cluster, "
        "'existing' connects to an existing scheduler",
    )
    scheduler_address: Optional[str] = Field(
        default=None,
        description="Address of existing Dask scheduler (e.g., 'tcp://localhost:8786')",
    )
    n_workers: int = Field(
        default=4,
        description="Number of workers for local cluster",
    )
    threads_per_worker: int = Field(
        default=2,
        description="Number of threads per worker",
    )
    memory_limit: str = Field(
        default="2GB",
        description="Memory limit per worker (e.g., '2GB', '4GB')",
    )

    @contextmanager
    def get_client(self) -> Iterator[Client]:
        """Get a Dask client, managing cluster lifecycle.

        Yields:
            A Dask distributed Client connected to either a local or existing cluster.

        Example:
            ```python
            @asset
            def my_asset(dask: DaskResource):
                with dask.get_client() as client:
                    ddf = dd.read_csv("data.csv")
                    result = ddf.compute()
                return result
            ```
        """
        if self.cluster_type == "local":
            cluster = LocalCluster(
                n_workers=self.n_workers,
                threads_per_worker=self.threads_per_worker,
                memory_limit=self.memory_limit,
            )
            client = Client(cluster)
            try:
                yield client
            finally:
                client.close()
                cluster.close()
        else:
            if not self.scheduler_address:
                raise ValueError(
                    "scheduler_address must be provided when cluster_type='existing'"
                )
            client = Client(self.scheduler_address)
            try:
                yield client
            finally:
                client.close()

    def create_local_client(self) -> Client:
        """Create a simple local Dask client without cluster management.

        This is useful for quick operations where you don't need the full
        distributed scheduler overhead.

        Returns:
            A Dask Client connected to a synchronous scheduler.
        """
        return Client(processes=False)
