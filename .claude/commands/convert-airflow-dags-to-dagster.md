# Convert Airflow DAGs to Dagster

You are helping a data engineer convert a set of Airflow DAGs into Dagster assets using the Components architecture. This workflow balances **asset-centric thinking** (data outputs) with **faithful operator replication** (preserving execution logic).

## Core Philosophy: Assets + Operator Fidelity

**Two key principles:**
1. **Think in Data Products**: Model data outputs as assets, not computation steps
2. **Replicate Operator Logic**: Preserve the exact execution behavior of Airflow operators

⚠️ **CRITICAL**: Do not oversimplify! Airflow operators contain complex execution logic that must be preserved.

## Workflow Overview

0. **Analyze Airflow operator implementations** (CRITICAL - do this first!)
1. Initialize Dagster project
2. Identify data outputs AND operator execution patterns
3. Create components that replicate operator logic
4. Implement asset checks for validation tasks
5. Test and validate the conversion

## Step 0: Analyze Airflow Operator Implementations (CRITICAL)

**Before designing components, you MUST understand what the operators actually do.**

### 0.1 Locate Custom Operators

Look for custom operator implementations in:
- `dags/operators/`
- `plugins/operators/`
- `common/operators/`
- Any file with `Operator` in the name

**Example paths:**
```
dags/acme/operators/dwh_operators.py
plugins/custom_operators.py
```

### 0.2 Read and Understand Operator Logic

For EACH operator used in the DAGs, read the `execute()` method to understand:

1. **Connection patterns**: Does it connect to one database, API, filesystem, or multiple?
2. **DML operations**: Are there DELETE/INSERT/UPDATE patterns?
3. **Pre/post operations**: Does it run any logic before or after main logic?
4. **Data movement**: Does it query one DB and insert into another?
5. **Parameter handling**: What parameters does it accept and use?
6. **Audit tracking**: Does it track audit IDs or metadata?

**Example Analysis:**

```python
# Read this CAREFULLY
class PostgresToPostgresOperator(BaseOperator):
    def execute(self, context):
        # 1. Connect to SOURCE database
        src_pg = PostgresHook(postgres_conn_id=self.src_postgres_conn_id)

        # 2. Execute SELECT query on source
        cursor = src_pg.get_conn().cursor()
        cursor.execute(self.sql, self.parameters)

        # 3. Connect to DESTINATION database
        dest_pg = PostgresHook(postgres_conn_id=self.dest_postgress_conn_id)

        # 4. Run PRE-OPERATOR (typically DELETE)
        if self.pg_preoperator:
            dest_pg.run(self.pg_preoperator)

        # 5. INSERT query results into destination
        dest_pg.insert_rows(table=self.pg_table, rows=cursor)

        # 6. Run POST-OPERATOR (if provided)
        if self.pg_postoperator:
            dest_pg.run(self.pg_postoperator)
```

**Your analysis should note:**
- ✅ This is a CROSS-DATABASE operator (source → destination)
- ✅ It executes pre-operator SQL (DELETE pattern for partition cleanup)
- ✅ It queries source, then inserts into destination
- ✅ It supports post-operator SQL
- ✅ Component must replicate ALL these steps, not just "produce a table"

### 0.3 Document Operator Patterns

Create a table of operator patterns BEFORE designing components:

| Operator Class | What It Actually Does | Critical Features to Preserve |
|----------------|----------------------|-------------------------------|
| `PostgresToPostgresOperator` | 1. Query source DB<br>2. DELETE from dest<br>3. INSERT to dest | Cross-DB, pre-operator, row-by-row insert |
| `PostgresOperatorWithTemplatedParams` | Execute SQL with parameters | Templated params, single DB |
| `S3ToRedshiftOperator` | COPY from S3 to Redshift | COPY command, credential passing |
| `PythonOperator` | Execute Python function | Function execution, return value handling |

### 0.4 Common Operator Anti-Patterns to Avoid

❌ **DON'T DO THIS:**
```python
# BAD: Oversimplified component that just executes SQL
class DatabaseTableAsset:
    def build_defs(self):
        @asset
        def table_asset():
            # This is too simple!
            execute_sql(self.sql)  # Missing: pre-operator, cross-DB, audit tracking
```

✅ **DO THIS:**
```python
# GOOD: Replicates actual operator behavior
class PostgresToPostgresAsset:
    def build_defs(self):
        @asset
        def etl_asset():
            # 1. Connect to SOURCE
            source_conn = connect_to_source()
            rows = source_conn.execute(self.source_sql)

            # 2. Connect to DESTINATION
            dest_conn = connect_to_destination()

            # 3. Run pre-operator (DELETE)
            if self.pre_operator:
                dest_conn.execute(self.pre_operator)

            # 4. INSERT rows
            dest_conn.insert_rows(self.destination_table, rows)

            # 5. Run post-operator
            if self.post_operator:
                dest_conn.execute(self.post_operator)
```

## Step 1: Initialize Dagster Project

Use the dagster-init skill to create a new Dagster project.

## Step 2: Analyze DAGs for Data Outputs AND Execution Patterns

For each task in the DAGs, document:

### 2.1 Data Output Analysis (Asset-Centric)

| Airflow Task | Data Output | Storage Location | Asset Name |
|--------------|-------------|------------------|------------|
| `extract_customer` | Customer records | `staging.customer` table | `staging_customer` |
| `process_dim` | Customer dimension | `dwh.dim_customer` table | `customer_dimension` |

### 2.2 Operator Execution Analysis (Fidelity-Focused)

| Airflow Task | Operator Used | Execution Pattern | Required Component Features |
|--------------|---------------|-------------------|----------------------------|
| `extract_customer` | `PostgresToPostgresOperator` | Query OLTP → DELETE staging → INSERT staging | Cross-DB, pre-operator support |
| `process_dim` | `PostgresOperatorWithTemplatedParams` | Execute SCD Type 2 SQL | Single DB, templated parameters |

### 2.3 Identify Critical Execution Details

For each task, document:

**Example: orders_staging DAG**

```python
# Airflow task definition
extract_orderinfo = PostgresToPostgresOperator(
    sql='select_order_info.sql',
    pg_table='staging.order_info',
    src_postgres_conn_id='postgres_oltp',      # ← Source DB
    dest_postgress_conn_id='postgres_dwh',     # ← Destination DB
    pg_preoperator="DELETE FROM staging.order_info WHERE partition_dtm >= '{{ ds }}'",  # ← CRITICAL: DELETE
    pg_postoperator=None,
    parameters={"window_start_date": "{{ ds }}", "audit_id": "{{ ti.xcom_pull(...) }}"},
    task_id='extract_orderinfo',
)
```

**Your migration checklist for this task:**
- [ ] Component must support cross-database connections
- [ ] Component must execute pre-operator (DELETE) before INSERT
- [ ] Component must support templated parameters (window_start_date, audit_id)
- [ ] Component must preserve audit tracking
- [ ] Asset represents: `staging.order_info` table

## Step 3: Create Components That Replicate Operator Logic

**Rule:** One component type per Airflow operator pattern, NOT one component per asset type.

Use the create-custom-dagster-component skill for this step.

### 3.1 Component Mapping Strategy

| Airflow Operator | Dagster Component | Why? |
|------------------|-------------------|------|
| `PostgresToPostgresOperator` | `PostgresToPostgresAsset` | Cross-DB ETL with pre/post operators |
| `PostgresOperatorWithTemplatedParams` | `SQLTransformAsset` | Single-DB SQL execution |
| `S3Hook` + `S3ToRedshiftOperator` | `S3ToWarehouseAsset` | S3 → warehouse COPY pattern |
| `PythonOperator` (with data output) | `PythonTransformAsset` | Python function execution |
| `BashOperator` (with data output) | `ScriptAsset` | Script execution pattern |

### 3.2 Component Design Requirements

When creating a component, it MUST replicate the operator's execution flow:

**Template for Component Design:**

```
Component: PostgresToPostgresAsset

Replicates: airflow.providers.postgres.operators.PostgresToPostgresOperator

Execution Flow:
1. Connect to source database (source_conn_id)
2. Execute SELECT query from source_sql_path
3. Fetch all rows from query result
4. Connect to destination database (dest_conn_id)
5. Execute pre_operator SQL (typically DELETE for partition cleanup)
6. Insert rows into destination_table
7. Execute post_operator SQL (if provided)
8. Return metadata: rows_extracted, rows_deleted, rows_inserted

Parameters:
- asset_key: Dagster asset identifier
- source_sql_path: Path to SELECT query
- destination_table: Target table (schema.table)
- pre_operator: SQL to run before insert (e.g., DELETE)
- post_operator: SQL to run after insert (optional)
- audit_key: Business key for tracking
- demo_mode: Simulate without DB connections

Metadata Tracked:
- rows_extracted: Count from source query
- rows_deleted: Count from pre_operator
- rows_inserted: Count inserted to destination
- audit_id: Unique run identifier
```

### 3.3 Create Components Using create-custom-dagster-component Skill

**Example Prompt for Component Creation:**

```
Create a component called PostgresToPostgresAsset that replicates Airflow's PostgresToPostgresOperator.

CRITICAL REQUIREMENTS:
1. Must connect to TWO databases: source (OLTP) and destination (DWH)
2. Must execute SELECT query on source database
3. Must run pre_operator SQL on destination (typically DELETE statement for partition cleanup)
4. Must insert query results row-by-row into destination table
5. Must support post_operator SQL on destination (optional)
6. Must track audit_key and use Dagster run_id as audit_id
7. Must support demo_mode that simulates execution without real connections

Parameters:
- asset_key: str
- source_sql_path: str (path to SELECT query file)
- destination_table: str (schema.table)
- pre_operator: Optional[str] (DELETE SQL)
- post_operator: Optional[str] (optional SQL)
- audit_key: Optional[str]
- demo_mode: bool = True
- deps: list[str] = []

In production mode, use environment variables:
- POSTGRES_SOURCE_HOST, POSTGRES_SOURCE_DB, etc. (for OLTP)
- POSTGRES_DEST_HOST, POSTGRES_DEST_DB, etc. (for DWH)

Metadata to track:
- rows_extracted, rows_deleted, rows_inserted, audit_id, audit_key
```

### 3.4 Verify Component Replicates Operator Logic

After creating a component, verify it matches the operator:

**Checklist:**
- [ ] Same number of database connections
- [ ] Same SQL execution order
- [ ] Pre-operator SQL executed in correct place
- [ ] Post-operator SQL executed (if applicable)
- [ ] Parameters handled identically
- [ ] Audit tracking preserved
- [ ] Metadata tracked matches operator behavior

## Step 4: Map Airflow Tasks to Component Instances

### 4.1 Create YAML Definitions That Preserve Operator Behavior

**Airflow Task:**
```python
extract_customer = PostgresToPostgresOperator(
    sql='select_customer.sql',
    pg_table='staging.customer',
    src_postgres_conn_id='postgres_oltp',
    dest_postgress_conn_id='postgres_dwh',
    pg_preoperator="DELETE FROM staging.customer WHERE partition_dtm >= DATE '{{ ds }}'",
    parameters={"window_start_date": "{{ ds }}"},
    task_id='extract_customer',
)
```

**Dagster YAML (must preserve ALL behavior):**
```yaml
type: etl_migration.components.PostgresToPostgresAsset
attributes:
  asset_key: staging_customer
  destination_table: staging.customer
  source_sql_path: sql/select_customer.sql
  pre_operator: "DELETE FROM staging.customer WHERE partition_dtm >= %(window_start_date)s"
  audit_key: customer
  demo_mode: true
```

**Verify equivalence:**
- ✅ `sql='select_customer.sql'` → `source_sql_path: sql/select_customer.sql`
- ✅ `pg_table='staging.customer'` → `destination_table: staging.customer`
- ✅ `src_postgres_conn_id` → Handled by component (source connection)
- ✅ `dest_postgress_conn_id` → Handled by component (dest connection)
- ✅ `pg_preoperator="DELETE..."` → `pre_operator: "DELETE..."`
- ✅ `parameters={"window_start_date": ...}` → Handled by component
- ✅ Audit tracking → `audit_key: customer` + run_id

## Step 5: Handle Special Cases

### 5.1 Audit Operators

**Airflow Pattern:**
```python
get_audit_id = AuditOperator(
    task_id='get_audit_id',
    audit_key="orders",
    cycle_dtm="{{ ts }}",
)
extract_task = PostgresToPostgresOperator(
    parameters={"audit_id": "{{ ti.xcom_pull(task_ids='get_audit_id') }}"}
)
get_audit_id >> extract_task
```

**Dagster Alternative:**
- Use `context.run.run_id` as audit_id
- Include `audit_key` as component parameter
- No separate audit asset needed

**In Component:**
```python
audit_id = context.run.run_id
params["audit_id"] = audit_id
# Track audit_key in metadata
return MaterializeResult(metadata={"audit_id": audit_id, "audit_key": self.audit_key})
```

### 5.2 External Task Sensors

**Airflow Pattern:**
```python
wait_for_staging = ExternalTaskSensor(
    external_dag_id='customer_staging',
    external_task_id='extract_customer',
)
wait_for_staging >> process_dimension
```

**Dagster Alternative:**
```yaml
# No sensor needed - use asset dependency
type: etl_migration.components.SQLTransformAsset
attributes:
  asset_key: customer_dimension
  deps:
    - staging_customer  # Automatic waiting
```

### 5.3 Validation Tasks → Asset Checks

**Airflow Pattern:**
```python
validate_schema = PythonOperator(
    task_id='validate_schema',
    python_callable=check_schema,
)
extract >> validate_schema >> transform
```

**Dagster Pattern:**
```python
@asset_check(asset=staging_customer)
def validate_customer_schema():
    return AssetCheckResult(passed=check_schema())
```

## Step 6: Consolidate and Organize

### 6.1 Maintain 1:1 DAG-to-Job Mapping (Always)

In order to maintain the DAG structure, always create a scheduled job component that selects all assets from a specific DAG.

Reference: https://github.com/dagster-io/hooli-data-eng-pipelines/blob/master/hooli-data-eng/src/hooli_data_eng/components/scheduled_job_component.py

All component YAML files need to be named `defs.yaml`, within folders in the `defs/` directory

### 6.2 Group Assets by DAG

Create one YAML file per Airflow DAG:

```
src/project/defs/
├── customer_staging/defs.yaml    # All assets from customer_staging DAG
├── orders_staging/defs.yaml       # All assets from orders_staging DAG
├── process_dimensions/defs.yaml   # All assets from process_dimensions DAG
└── process_facts/defs.yaml        # All assets from process_order_fact DAG
```

Use `---` to separate multiple component instances in one file.


## Step 7: Validation and Testing

### 7.1 Validate Component Logic Replication

For each component, manually verify against the original operator:

```bash
# Compare side-by-side
# Airflow: dags/operators/custom_operators.py
# Dagster: src/project/components/custom_asset.py
```

**Checklist per component:**
- [ ] Same database connections (source/dest if applicable)
- [ ] Same execution order
- [ ] Pre-operator executed at correct time
- [ ] Post-operator executed at correct time
- [ ] The logic in the python functions is preserved in the new component
- [ ] Parameters passed identically
- [ ] Metadata matches operator output

### 7.2 Test Demo Mode

```bash
uv run dg check defs
uv run dg list defs
uv run dg dev
```

In demo mode, verify:
- Components log what they WOULD do
- Pre-operator SQL is logged
- Post-operator SQL is logged (if applicable)
- Metadata is simulated realistically
- No actual database connections

### 7.3 Test Production Mode (with test databases)

1. Set up test OLTP and DWH databases
2. Disable demo_mode in YAML
3. Set environment variables for connections
4. Materialize one staging asset
5. Verify:
   - Source DB is queried
   - Destination pre-operator (DELETE) runs
   - Rows are inserted into destination
   - Metadata shows actual counts

### 7.4 Verify production mode

Ensure that the production mode python logic matches the logic in the original tasks.

## Step 8: Documentation

### 8.1 Operator → Component Mapping

Document how each Airflow operator maps to Dagster components:

| Airflow Operator | Execution Pattern | Dagster Component | Key Features Preserved |
|------------------|-------------------|-------------------|----------------------|
| `PostgresToPostgresOperator` | Cross-DB ETL with pre-op | `PostgresToPostgresAsset` | Source query, DELETE, INSERT, audit |
| `PostgresOperatorWithTemplatedParams` | Single-DB SQL | `SQLTransformAsset` | Templated params, SCD Type 2 |

### 8.2 Side-by-Side Comparisons

Include concrete examples showing equivalence:

```markdown
## Airflow orders_staging DAG

\`\`\`python
extract_orderinfo = PostgresToPostgresOperator(
    sql='select_order_info.sql',
    pg_table='staging.order_info',
    pg_preoperator="DELETE FROM staging.order_info WHERE...",
)
\`\`\`

## Dagster staging_order_info Asset

\`\`\`yaml
type: project.components.PostgresToPostgresAsset
attributes:
  asset_key: staging_order_info
  destination_table: staging.order_info
  source_sql_path: sql/select_order_info.sql
  pre_operator: "DELETE FROM staging.order_info WHERE..."
\`\`\`

**Equivalence verified:**
- ✅ Same SQL query
- ✅ Same destination table
- ✅ Same pre-operator (DELETE)
- ✅ Same execution flow
```

## Success Criteria

The conversion is complete when:

### Functional Requirements
- ✅ Every Airflow operator pattern has a corresponding Dagster component
- ✅ Components replicate operator execution logic (not just data outputs)
- ✅ Pre-operator SQL (DELETE patterns) is preserved
- ✅ Post-operator SQL is preserved (where applicable)
- ✅ Cross-database operations work correctly
- ✅ Audit tracking is maintained (audit_key + run_id)
- ✅ parameters are handled identically
- ✅ Asset dependencies match DAG dependencies

### Testing Requirements
- ✅ `uv run dg check defs` passes
- ✅ `uv run dg list defs` shows all assets
- ✅ Demo mode works without databases
- ✅ Production mode has the same connections and logic
- ✅ Any python function is that reused in the Dagster asset creation is made into a component
- ✅ Metadata tracking matches operator behavior

### Documentation Requirements
- ✅ Operator → Component mapping documented
- ✅ Side-by-side comparisons provided
- ✅ Execution flow differences explained
- ✅ Critical features preservation verified
- ✅ Comparison of tasks to assets per DAG
- ✅ Breakdown of whether the assets follow a component or a pythonic pattern

## Critical Anti-Patterns to Avoid

### ❌ Anti-Pattern 1: Oversimplifying Operator Logic

**DON'T:**
```python
# This misses critical execution details
class DatabaseTableAsset:
    def build_defs(self):
        @asset
        def table():
            execute_sql(self.sql)  # Too simple!
```

**DO:**
```python
# This replicates actual operator behavior
class PostgresToPostgresAsset:
    def build_defs(self):
        @asset
        def etl_asset():
            # Step 1: Query source
            source_rows = query_source_db(self.source_sql)
            # Step 2: DELETE from destination
            delete_from_dest(self.pre_operator)
            # Step 3: INSERT to destination
            insert_to_dest(source_rows)
```

### ❌ Anti-Pattern 2: Ignoring Pre/Post Operators

**DON'T:**
```yaml
# Missing pre_operator means data won't be cleaned before insert
attributes:
  sql_path: extract_orders.sql
  table_name: staging.orders
  # ← WHERE IS THE DELETE?
```

**DO:**
```yaml
# Preserves DELETE-before-INSERT pattern
attributes:
  source_sql_path: extract_orders.sql
  destination_table: staging.orders
  pre_operator: "DELETE FROM staging.orders WHERE partition_dtm >= %(window_start_date)s"
```

### ❌ Anti-Pattern 3: Missing Cross-Database Support

**DON'T:**
```python
# This only connects to ONE database
conn = connect_to_db()
conn.execute(self.sql)
```

**DO:**
```python
# This connects to BOTH source and destination
source_conn = connect_to_source_db()
dest_conn = connect_to_dest_db()
rows = source_conn.query(self.source_sql)
dest_conn.insert(rows)
```

### ❌ Anti-Pattern 4: Losing Audit Tracking

**DON'T:**
```python
# No audit information tracked
return MaterializeResult()
```

**DO:**
```python
# Preserves audit tracking
return MaterializeResult(metadata={
    "audit_id": context.run.run_id,
    "audit_key": self.audit_key,
    "rows_extracted": row_count,
})
```

## Final Checklist

Before completing the migration, verify:

- [ ] Read and understood ALL custom operator implementations
- [ ] Created components that replicate operator execution flows
- [ ] Preserved pre-operator SQL (DELETE patterns)
- [ ] Preserved post-operator SQL (where applicable)
- [ ] Handled parameters identically to Airflow
- [ ] Tested demo mode (simulates execution)
- [ ] Verified production mode (python logic and connections to real databases and APIs are present)
- [ ] Documented operator → component mapping
- [ ] Documented a comparison of Airflow tasks to Dagster Assets
- [ ] Document that all production mode assets maintain their production logic
- [ ] Verify that all Dagster assets are created using components if their logic is invoked more than once
- [ ] Provided side-by-side comparisons
- [ ] Verified metadata tracking matches operators
