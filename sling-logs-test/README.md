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
