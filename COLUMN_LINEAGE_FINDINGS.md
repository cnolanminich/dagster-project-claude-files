# Column-Level Lineage Investigation: Dask + Dagster

## Executive Summary

This document summarizes the investigation into whether Dask provides sufficient information to populate column-level lineage in Dagster.

**TL;DR: Dask provides excellent schema metadata but does NOT natively support column-level lineage tracking. Column lineage must be implemented explicitly by the developer.**

## What Dask Provides

### Schema Information (Well Supported)

Dask provides robust schema introspection through its `_meta` attribute:

```python
import dask.dataframe as dd

ddf = dd.read_csv("data.csv")

# Available WITHOUT computation:
ddf.columns          # Column names
ddf.dtypes           # Column data types
ddf._meta            # Empty pandas DataFrame with correct schema
ddf.index.name       # Index name
ddf.index.dtype      # Index data type
ddf.npartitions      # Number of partitions
ddf.known_divisions  # Whether division boundaries are known
```

This metadata is sufficient to populate Dagster's `TableSchema` and `TableColumn` objects.

### Row Counts (Requires Computation)

```python
# These REQUIRE computation (trigger full data load):
len(ddf)                              # Total row count
ddf.map_partitions(len).compute()     # Row count per partition

# This does NOT require computation:
ddf.npartitions                       # Number of partitions (not rows)
```

### Task Graph (Not Column-Aware)

Dask maintains a task graph that tracks computational dependencies:

```python
ddf.__dask_graph__()   # The task graph
ddf.__dask_keys__()    # Output keys
```

**Important**: The task graph tracks dependencies at the **partition/chunk level**, not at the **column level**. It knows which tasks depend on which, but NOT which columns flow into which output columns.

## What Dask Does NOT Provide

### Native Column-Level Lineage

Dask **does not track** how columns transform through operations. For example:

```python
# Dask doesn't know that 'total' depends on 'price' and 'quantity'
ddf['total'] = ddf['price'] * ddf['quantity']

# Dask doesn't know that 'full_name' depends on 'first' and 'last'
ddf['full_name'] = ddf['first'] + ' ' + ddf['last']
```

### Why Not?

1. **Design Philosophy**: Dask is a parallel computing library, not a data governance tool
2. **Performance**: Tracking column lineage would add overhead to every operation
3. **Complexity**: Some operations (UDFs, complex joins) make lineage inference difficult
4. **Scope**: Column lineage is typically handled by orchestration or governance layers

## Recommended Approach for Dagster

### 1. Extract Schema Automatically

```python
from dagster import TableSchema, TableColumn

def extract_schema_from_dask(ddf):
    """Extract TableSchema from Dask DataFrame metadata."""
    columns = []
    for col_name in ddf._meta.columns:
        dtype = ddf._meta[col_name].dtype
        columns.append(TableColumn(name=str(col_name), type=str(dtype)))
    return TableSchema(columns=columns)
```

### 2. Track Column Lineage Explicitly

Since Dask doesn't provide column lineage, you must track it manually:

```python
from dagster import TableColumnLineage, TableColumnDep, AssetKey

class ColumnLineageTracker:
    def __init__(self):
        self._deps = {}

    def add_column_dep(self, output_col, source_asset, source_cols):
        if output_col not in self._deps:
            self._deps[output_col] = []
        for src in source_cols:
            self._deps[output_col].append(
                TableColumnDep(asset_key=AssetKey(source_asset), column_name=src)
            )

    def get_lineage(self):
        return TableColumnLineage(deps_by_column=self._deps)

# Usage:
tracker = ColumnLineageTracker()
ddf['total'] = ddf['price'] * ddf['quantity']
tracker.add_column_dep('total', 'raw_orders', ['price', 'quantity'])
```

### 3. Return Comprehensive Metadata

```python
@asset
def my_asset(dask: DaskResource):
    # ... process data ...

    return MaterializeResult(
        metadata={
            "dagster/column_schema": extract_schema_from_dask(ddf),
            "dagster/row_count": len(result),
            "dagster/column_lineage": tracker.get_lineage(),
        }
    )
```

## Comparison with Other Tools

| Feature | Dask | Spark | dbt | Databricks |
|---------|------|-------|-----|------------|
| Schema Metadata | Yes (`_meta`) | Yes (schema) | Yes | Yes |
| Row Count | Requires compute | Requires compute | Post-run | Post-run |
| Column Lineage | No | No (native) | Yes (SQL parsing) | Yes (Unity Catalog) |
| Task/Job Lineage | Yes (task graph) | Yes (DAG) | Yes | Yes |

### Why dbt Has Column Lineage

dbt can infer column lineage because:
1. It uses SQL, which has well-defined semantics
2. SQL can be parsed to extract column references
3. dbt controls the execution and can instrument queries

Dask uses arbitrary Python code, which cannot be statically analyzed for column dependencies.

## Alternative Approaches

### 1. SQL-Based Processing

If column lineage is critical, consider using SQL-based tools (dbt, SQLMesh) where lineage can be automatically inferred.

### 2. Wrapper Libraries

Some libraries attempt to track DataFrame transformations:
- **lineage** (experimental)
- **data-lineage** (limited support)

These are not production-ready and have significant limitations.

### 3. Custom Instrumentation

Build a wrapper around Dask operations that logs column dependencies:

```python
class TrackedDataFrame:
    def __init__(self, ddf, tracker):
        self._ddf = ddf
        self._tracker = tracker

    def __getitem__(self, key):
        # Track column access
        return self._ddf[key]

    def __setitem__(self, key, value):
        # Would need to parse 'value' expression
        # This is where it gets complex/impossible
        self._ddf[key] = value
```

This approach is fragile and doesn't handle complex expressions.

## Conclusion

### Dask's Strengths for Dagster
- Excellent schema metadata extraction (`_meta`)
- Parallel processing capabilities
- Works well with Dagster's resource model
- Easy to surface schema in Dagster UI

### Dask's Limitations
- No native column-level lineage
- Row counts require computation
- Task graph is partition-level, not column-level

### Recommendations

1. **Use Dask for parallel processing** - it's excellent at this
2. **Extract schemas automatically** - Dask provides everything needed
3. **Track column lineage explicitly** - build it into your asset code
4. **Consider SQL for lineage-critical paths** - dbt/SQLMesh can auto-infer
5. **Document lineage requirements** - make it part of asset development standards

## References

- [Dagster Column-Level Lineage Docs](https://docs.dagster.io/guides/build/assets/metadata-and-tags/column-level-lineage)
- [Dagster Table Schema Docs](https://docs.dagster.io/guides/build/assets/metadata-and-tags/table-metadata)
- [Dask DataFrame Design](https://docs.dask.org/en/stable/dataframe-design.html)
- [Understanding Dask's meta keyword](https://blog.dask.org/2022/08/09/understanding-meta-keyword-argument)
