# Modify Existing Dagster Project

You are helping a sales engineer modify an existing Dagster demonstration project for a prospective client. The user should provide:
1. The path to the existing Dagster project directory
2. The modifications or enhancements they want to make

If these are not provided, ask the user for them.

## Understanding the Existing Project

Before making any modifications, thoroughly understand the existing project structure:

### Step 1: Explore the Project Structure

Navigate to the provided project directory and explore:

```bash
cd <project_path>
```

1. **Check the project structure**:
   ```bash
   tree -L 3 -I '__pycache__|*.pyc|.venv|.git'
   ```

2. **Identify the defs location**:
   ```bash
   find . -name "defs.py" -o -name "defs.yaml" | head -10
   ```

3. **List existing components**:
   ```bash
   uv run dg list components
   ```

4. **List existing asset definitions**:
   ```bash
   uv run dg list defs
   ```

5. **Check for any existing YAML configurations**:
   ```bash
   find . -name "*.yaml" -o -name "*.yml" | grep -v node_modules | grep -v .venv
   ```

### Step 2: Understand Current Assets

Use the Task tool with subagent_type=Explore to understand:
- What assets currently exist
- How they're connected (dependencies)
- Which technologies/integrations are being used
- Whether demo_mode is already implemented
- The overall data flow

Read key files to understand:
- Component implementations
- Asset definitions
- Configuration files

## Making Modifications

Based on the user's requirements and your understanding of the project, proceed with modifications:

### Common Modification Patterns

#### A. Adding New Integrations/Components

If adding a new technology integration:

1. **Use existing components when possible**:
   ```
   Use the "use-or-subclass-existing-component" skill
   ```
   - Search for Dagster's built-in components (dbt, Fivetran, Airbyte, etc.)
   - Create component instances with proper configurations
   - Ensure demo_mode is supported

2. **Create custom components for non-standard integrations**:
   ```
   Use the "create-custom-dagster-component" skill
   ```
   - Follow Dagster component best practices
   - Include both real and demo_mode implementations
   - Add realistic asset structures

#### B. Extending Existing Assets

When modifying existing assets:

1. **Read the existing component code carefully**
2. **Maintain the existing asset structure and naming conventions**
3. **Preserve existing dependencies**
4. **Add new assets that fit logically into the data flow**

Example asset flow to maintain/extend:
- Raw data ingestion → Cleaning/transformation → Business logic → Analytics/ML → Output

#### C. Enhancing Demo Mode

If demo_mode doesn't exist or needs improvement:

1. **Add demo_mode parameter to component YAML configs**:
   ```yaml
   demo_mode: true  # or false for real mode
   ```

2. **Implement demo_mode in component Python code**:
   ```python
   @dg.component(...)
   class MyComponent:
       def __init__(self, demo_mode: bool = False):
           self.demo_mode = demo_mode

       def execute(self, context):
           if self.demo_mode:
               # Return mock/sample data
               return generate_demo_data()
           else:
               # Real implementation
               return fetch_real_data()
   ```

3. **Create realistic demo data** that represents actual use cases

#### D. Adding Schedules and Sensors

To add automation:

1. **Add schedules for time-based execution**:
   ```python
   from dagster import ScheduleDefinition

   daily_schedule = ScheduleDefinition(
       name="daily_refresh",
       target=my_job,
       cron_schedule="0 0 * * *",
   )
   ```

2. **Add sensors for event-driven workflows**:
   ```python
   from dagster import sensor, RunRequest

   @sensor(job=my_job)
   def my_sensor(context):
       # Sensor logic
       yield RunRequest(...)
   ```

#### E. Improving Asset Metadata and Documentation

Enhance existing assets with:

1. **Better descriptions and metadata**:
   ```python
   @asset(
       description="Detailed description of what this asset does",
       metadata={
           "data_source": "PostgreSQL",
           "refresh_frequency": "Daily",
           "owner": "data-team",
       }
   )
   def my_asset(context):
       ...
   ```

2. **Asset checks for data quality**:
   ```python
   from dagster import asset_check, AssetCheckResult

   @asset_check(asset=my_asset)
   def check_row_count(context):
       # Validation logic
       return AssetCheckResult(passed=True)
   ```

## Best Practices for Modifications

### Component Design Principles

1. **Use Dagster Components** - Don't write raw @asset definitions when components exist
2. **Support Demo Mode** - All components should work without real credentials
3. **Realistic Asset Structure** - Match real-world data flows (3-5 assets per component)
4. **Proper Dependencies** - Use deps parameter or input parameters to connect assets
5. **Descriptive Naming** - Use clear, business-oriented names

### Configuration Management

1. **Use YAML for configuration** - Keep configs in defs.yaml files
2. **Separate credentials** - Use environment variables for sensitive data
3. **Document parameters** - Add comments to YAML explaining options

### Code Quality

1. **Follow existing patterns** - Match the style of the existing codebase
2. **Don't break existing functionality** - Preserve working features
3. **Add proper error handling** - Especially in demo_mode switches
4. **Include type hints** - Make code more maintainable

## Step 3: Validation

After making modifications, validate the changes:

### Check Definitions Load

```bash
uv run dg check defs
```

This must pass without errors.

### List All Definitions

```bash
uv run dg list defs
```

Verify all expected assets appear.

### Test Demo Mode

If demo_mode was added/modified:

1. Set demo_mode: true in relevant YAML files
2. Run `uv run dg check defs` again
3. Try materializing an asset in the Dagster UI:
   ```bash
   uv run dg dev
   ```

### Verify Real Connections

For real mode (demo_mode: false):

1. Ensure components connect to actual systems
2. Verify credentials are properly configured
3. Test that assets can materialize (if credentials available)

## Step 4: Documentation

After modifications, update project documentation:

1. **Update README** if adding major features
2. **Add comments** to new/modified code
3. **Document configuration options** in component YAML
4. **Note any new dependencies** in pyproject.toml

## Common Modification Scenarios

### Scenario 1: Adding a New Data Source

1. Explore for existing connectors/components
2. Add new component using appropriate skill
3. Create 2-3 assets for the new source
4. Connect new assets to existing downstream assets
5. Validate and test

### Scenario 2: Enhancing Existing Pipeline

1. Read existing asset definitions
2. Identify enhancement points
3. Add new transformation/enrichment assets
4. Update dependencies
5. Validate and test

### Scenario 3: Converting to Demo Mode

1. Audit all components for external dependencies
2. Add demo_mode parameter to YAML configs
3. Implement demo_mode logic in Python code
4. Create realistic mock data
5. Test both modes

### Scenario 4: Adding Data Quality Checks

1. Identify critical assets
2. Add @asset_check definitions
3. Implement validation logic
4. Test checks trigger appropriately

## Error Handling

If you encounter issues:

- **Import errors**: Check that all dependencies are in pyproject.toml
- **Component not found**: Verify component registration in defs.py
- **Asset won't load**: Check for syntax errors and proper dependencies
- **Demo mode not working**: Verify demo_mode parameter is properly passed through

## Success Criteria

Modifications are successful when:

1. ✅ `uv run dg check defs` passes without errors
2. ✅ All assets appear in `uv run dg list defs`
3. ✅ Demo mode works without real credentials (if applicable)
4. ✅ Real mode connects to actual systems (if applicable)
5. ✅ New/modified assets fit logically into the data flow
6. ✅ Existing functionality is preserved
7. ✅ Code follows project conventions

## Tips

- **Start small**: Make incremental changes and validate frequently
- **Preserve working code**: Don't refactor unnecessarily
- **Test both modes**: If demo_mode exists, test both true and false
- **Ask questions**: If requirements are unclear, ask the user for clarification
- **Use skills**: Leverage dagster-init, use-or-subclass-existing-component, and create-custom-dagster-component skills
- **Read before writing**: Understand the existing code before modifying it
