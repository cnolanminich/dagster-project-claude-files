"""Dagster Components for Dask DataFrame processing.

This package provides reusable Components that automatically handle:
- Column schema extraction
- Row count computation
- Dask cluster management
"""

from dagster_dask_lineage.components.dask_dataframe_component import (
    DaskAssetConfig,
    DaskClusterConfig,
    DaskDataFrameComponentLegacy,
    create_dask_metadata,
    extract_dask_schema,
)

# Only export Component if available (Dagster 1.9+)
try:
    from dagster_dask_lineage.components.dask_dataframe_component import (
        DaskDataFrameComponent,
    )
    __all__ = [
        "DaskDataFrameComponent",
        "DaskDataFrameComponentLegacy",
        "DaskClusterConfig",
        "DaskAssetConfig",
        "extract_dask_schema",
        "create_dask_metadata",
    ]
except ImportError:
    __all__ = [
        "DaskDataFrameComponentLegacy",
        "DaskClusterConfig",
        "DaskAssetConfig",
        "extract_dask_schema",
        "create_dask_metadata",
    ]
