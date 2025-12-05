# Create Dagster Demo Project

You are helping a sales engineer create a Dagster demonstration project for a prospective client. The context of the demo requirements, including the path for the dagster demo, should be given in the instruction that called this command. If not, please askt the user for theem.

Operating in the provided demo directory, follow this workflow:

## Step 1: Initialize Dagster Project

Use the dagster-init skill to create a new Dagster project.

## Step 2: Generate and Customize Demo Assets

Use the use or subclass existing component skill to find and create components for integrations that Dagster already has. If there isn't one, use the create customer dagster component skill to create those.

The goal of the demo Dagster project is to have:

- 3-5 realistic assets based on the chosen technologies
- Proper dependencies between assets
- Descriptive names
- **Asset keys designed for downstream consumption** (see below)


There should be both a real implementation and a `demo_mode` implementation that is runnable locally without access to the system and is activated by a `demo_mode` boolean flag in a component YAML.

Example asset structure:

- Raw data ingestion asset
- Data transformation/cleaning asset
- Business logic/aggregation asset
- ML model or analytics asset (if applicable)
- Output/export asset

### Critical: Design Asset Keys for Integration

When creating components, **design asset keys so downstream components can reference them naturally**:

1. **If dbt will consume assets**: Use flat, 2-level keys like `["source_name", "table"]`
   - Example: `["fivetran_raw", "customers"]` allows dbt to use `source('fivetran_raw', 'customers')`
   - Avoid: `["fivetran", "raw", "customers"]` requires extra configuration

2. **For dbt models consumed by reverse ETL**: Use simple model names
   - Example: `["customer_lifetime_value"]` is easy for Hightouch to reference
   - dbt naturally creates these from model file names

3. **For custom processing chains**: Use consistent 2-level patterns
   - Example: `["raw", "table"]` → `["processed", "table"]` → `["enriched", "table"]`

After creating all the components, validate that dagster loads by running

```bash
uv run dg check defs
```

and then use

```bash
uv run dg list defs
```

to make sure that all expected dependencies line up. This means that the deps field for a downstream asset should have the upstream asset as part of it. This can be especially tricky for integrations like sling and dbt.

### Verify Asset Key Alignment

Run this command to check dependencies are correct:

```bash
uv run dg list defs --json | uv run python -c "
import sys, json
data = json.load(sys.stdin)
assets = data.get('assets', [])
print('Asset Dependencies:\n')
for asset in assets:
    key = asset.get('key', 'unknown')
    deps = asset.get('deps', [])
    if deps:
        print(f'{key}')
        for dep in deps:
            print(f'  ← {dep}')
    else:
        print(f'{key} (no dependencies)')
    print()
"
```

**What to verify:**
- ✅ Downstream assets list upstream assets in their `deps` array
- ✅ No missing dependencies (e.g., dbt models should depend on their sources)
- ✅ Asset keys are simple and descriptive (typically 2 levels: `["category", "name"]`)
- ✅ Dependencies work in both demo mode and production mode

If dependencies are missing or incorrect:
- For **dbt models**: Ensure they use `{{ source('source_name', 'table') }}` in SQL
- For **custom components**: Check that `deps=[...]` or `@asset(deps=[...])` is set correctly
- For **Fivetran/API components**: Verify asset keys match what dbt sources expect

## Step 3: Validate Code

Ensure that the components are using real connections to database or API systems and not merely "passing."