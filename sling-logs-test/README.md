# Sling Logs Test Project

This project tests whether the `SlingReplicationCollectionComponent` produces the same style of logs as a Pythonic sling setup when running `dg launch --asset <asset_key>`.

## Project Structure

```
sling-logs-test/
├── data/
│   └── source.csv                           # Sample source data
├── src/sling_logs_test/
│   ├── definitions.py                       # Main definitions module
│   └── defs/
│       ├── component_sling/                 # Component-based approach
│       │   ├── defs.yaml                    # SlingReplicationCollectionComponent config
│       │   └── replication.yaml             # Sling replication config
│       ├── pythonic_assets.py               # Pythonic approach with @sling_assets
│       └── pythonic_replication.yaml        # Replication config for Pythonic approach
```

## Two Approaches Being Compared

### 1. Component-Based Approach (`component_sling`)
Uses `SlingReplicationCollectionComponent` with YAML configuration:
- Asset key: `component_sling_asset`
- Output: `/tmp/sling_component_output/users.csv`

### 2. Pythonic Approach (`pythonic_assets.py`)
Uses `@sling_assets` decorator with Python code:
- Asset key: `pythonic_sling_asset`
- Output: `/tmp/sling_pythonic_output/users.csv`

## Getting Started

### Installing dependencies

```bash
uv sync
```

### Verify Definitions Load

```bash
uv run dg check defs
```

### List All Assets

```bash
uv run dg list defs
```

## Running the Tests

### Launch Component-Based Asset

```bash
uv run dg launch --asset component_sling_asset 2>&1 | tee component_logs.txt
```

### Launch Pythonic Asset

```bash
uv run dg launch --asset pythonic_sling_asset 2>&1 | tee pythonic_logs.txt
```

### Compare Logs

```bash
diff component_logs.txt pythonic_logs.txt
```

## What to Look For in Logs

Key log messages to compare between the two approaches:

1. **Sling Binary Download Message**:
   ```
   Downloading sling binary (v1.x.x) for linux/amd64...
   ```
   This appears when sling is first invoked and needs to download its binary.

2. **Replication Messages**:
   ```
   Running Sling replication with command: ...
   ```

3. **Stream Processing Messages**:
   ```
   stream file://data/source.csv
   ```

4. **Row Count Messages**:
   ```
   inserted X rows into file:///tmp/...
   ```

5. **Execution Success Messages**:
   ```
   execution succeeded
   ```

## Expected Results

Both approaches should produce **identical** log output because:

1. Both use the same underlying `SlingResource.replicate()` method
2. Both process the same `replication.yaml` format
3. Both call the sling CLI binary in the same way

The `SlingReplicationCollectionComponent` internally builds a `@sling_assets` decorated function (see `component.py:187-201`), so the execution path converges at the `SlingResource.replicate()` method.

## Key Files for Log Generation

The log messages are generated in:
- `dagster_sling/resources.py` - `SlingResource._batch_sling_replicate()` and `_stream_sling_replicate()` methods
- `sling` Python package - `sling/_run()` function
- The sling CLI binary itself

## Differences You Might See

1. **Asset key names** will differ (as configured)
2. **Output file paths** will differ
3. **Timing/elapsed time** values will vary

The core sling execution logs should be **identical in style and content**.

## Running Dagster UI

Start the Dagster UI web server:

```bash
uv run dg dev
```

Open http://localhost:3000 in your browser to see the project.

## Learn more

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster Sling Integration](https://docs.dagster.io/integrations/sling)
- [Sling Documentation](https://docs.slingdata.io/)

## Investigation: Warning/Error Handling Differences

### Reported Issue
Users have reported that errors like the sling binary download failure are surfaced in the Pythonic version but not in the Components version, leading to a "perpetual execution state".

### Investigation Findings

#### 1. Warning Suppression Analysis
The `load_from_defs_folder()` function (used by both approaches in dg projects) is decorated with `@suppress_dagster_warnings`. However, testing shows that this decorator does **NOT** suppress `UserWarning` from the sling package - the warning is still issued.

The decorator only suppresses Dagster-specific warnings:
- `DeprecationWarning`
- `SupersessionWarning`
- `PreviewWarning`
- `BetaWarning`

#### 2. When the Sling Binary Warning is Issued
The sling binary download happens at **import time** when the `sling` module is first loaded (see `sling/bin.py:172-181`). If the download fails:
1. A `print()` statement outputs: "Downloading sling binary..."
2. A `warnings.warn()` is issued: "Failed to download sling binary: ..."
3. It attempts to find sling in PATH as a fallback

#### 3. Potential Differences in Execution Context

**Component Loading Path:**
```
load_from_defs_folder() → ComponentTree.build_defs() → SlingReplicationCollectionComponent.build_asset() → @sling_assets decorated function
```

**Pythonic Loading Path (outside dg projects):**
```
Direct Python module import → @sling_assets decorator → SlingResource.replicate()
```

When using a traditional Dagster setup (without dg/components), the Pythonic approach imports modules directly without the `@suppress_dagster_warnings` context, which may result in different warning visibility.

#### 4. Error Swallowing in Metadata Fetching
In `dagster_sling/sling_event_iterator.py`, the `fetch_row_count_metadata()` and `fetch_column_metadata()` functions have try/except blocks that log errors as warnings rather than raising them:
```python
except Exception as e:
    context.log.warning(f"Failed to fetch ... for stream %s\nException: {e}", ...)
```

This affects both approaches equally.

### Test Script
A test script `test_log_comparison.py` is included to help diagnose warning behavior in different loading scenarios:
```bash
uv run python test_log_comparison.py
```
