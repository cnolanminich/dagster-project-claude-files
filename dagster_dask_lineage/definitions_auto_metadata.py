"""Dagster definitions demonstrating AUTOMATIC metadata extraction.

This is the answer to: "How to automatically tell all Dagster assets
using dagster-dask to get the schema and row count?"

ANSWER: Use an IOManager that extracts metadata in handle_output().

When you configure DaskParquetIOManager as your io_manager, ALL assets
that return Dask DataFrames will automatically get:
- dagster/column_schema
- dagster/row_count
- npartitions

NO manual MaterializeResult or add_output_metadata needed!
"""

from dagster import Definitions, load_assets_from_modules

from dagster_dask_lineage import assets_with_io_manager
from dagster_dask_lineage.io_manager import DaskParquetIOManager

# Load assets that use automatic metadata extraction
auto_assets = load_assets_from_modules([assets_with_io_manager])

# Configure the IOManager - this is where the magic happens!
# When you set io_manager to DaskParquetIOManager, ALL assets that return
# dd.DataFrame will automatically get schema and row count metadata.
resources = {
    "io_manager": DaskParquetIOManager(
        base_path="/tmp/dagster_dask_warehouse",
        compute_row_count=True,  # Automatically compute and attach row count
    ),
}

defs = Definitions(
    assets=auto_assets,
    resources=resources,
)


# =============================================================================
# HOW THIS COULD BE ADDED TO DAGSTER-DASK
# =============================================================================
#
# The dagster-dask library could add:
#
# 1. Utility function (like dagster-pandas):
#    from dagster_dask import create_table_schema_metadata_from_dask_dataframe
#
# 2. IOManager with auto-metadata:
#    from dagster_dask import DaskParquetIOManager
#
# 3. Integration with existing executor:
#    Jobs using dask_executor could automatically use the IOManager
#
# This would make it trivial for users to get schema/row count for all
# Dask-based assets without any manual instrumentation.
#
# =============================================================================
"""
