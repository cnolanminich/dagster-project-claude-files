# Expose `generate_cli_args` on `DbtProject` and `DbtProjectComponent`

## Problem

`DagsterDbtProjectPreparer` already accepts `generate_cli_args` (default
`["parse", "--quiet"]`), but `DbtProject.__new__` hardcodes
`DagsterDbtProjectPreparer()` with no way to override it.  This means users who
need `dbt compile` (e.g. to get `node.compiled_code` in the manifest) must
subclass `DbtProjectComponent` and run compile as a second invocation after
parse.

## Goal

Let users optionally pass `generate_cli_args` through YAML all the way to the
preparer, defaulting to `["parse", "--quiet"]` (current behaviour unchanged).

```yaml
# defs.yaml — new optional field
type: dagster_dbt.DbtProjectComponent
attributes:
  project:
    project_dir: jaffle_shop
    generate_cli_args: ["compile", "--quiet"]   # opt-in
```

---

## Changes (3 files)

### 1. `dagster_dbt/dbt_project.py` — `DbtProject.__new__`

Add an optional `generate_cli_args` parameter and forward it to the preparer.

```python
# dagster_dbt/dbt_project.py

class DbtProject(NamedTuple):
    ...
    # existing fields unchanged

    def __new__(
        cls,
        project_dir: Path | str,
        *,
        target_path: Path | str = Path("target"),
        profiles_dir: Path | str | None = None,
        profile: str | None = None,
        target: str | None = None,
        packaged_project_dir: Path | str | None = None,
        state_path: Path | str | None = None,
+       generate_cli_args: Sequence[str] | None = None,
    ) -> "DbtProject":
        ...

-       preparer = DagsterDbtProjectPreparer()
+       preparer = DagsterDbtProjectPreparer(generate_cli_args=generate_cli_args)

        ...
```

This is fully backwards-compatible: `None` falls through to the existing
`or ["parse", "--quiet"]` default inside `DagsterDbtProjectPreparer.__init__`.

---

### 2. `dagster_dbt/components/dbt_project/component.py` — `DbtProjectArgs`

Add the field to the dataclass so it can be resolved from YAML.

```python
# dagster_dbt/components/dbt_project/component.py

@dataclass
class DbtProjectArgs(dg.Resolvable):
    """Aligns with DbtProject.__new__."""

    project_dir: str
    target_path: str | None = None
    profiles_dir: str | None = None
    profile: str | None = None
    target: str | None = None
    packaged_project_dir: str | None = None
    state_path: str | None = None
+   generate_cli_args: list[str] | None = None
```

No other changes needed — the existing passthrough in
`DbtProjectArgsManager.get_project` already forwards all non-None kwargs:

```python
# dbt_project_manager.py  (no changes needed here)
def get_project(self, state_path: Path | None) -> "DbtProject":
    kwargs = asdict(self.args)
    ...
    return DbtProject(
        project_dir=project_dir,
        **{k: v for k, v in kwargs.items() if v is not None and k != "project_dir"},
    )
```

Because `generate_cli_args` is `None` by default, the `if v is not None` filter
means it only gets passed when explicitly set — no behaviour change for existing
users.

---

### 3. `dagster_dbt/components/dbt_project/component.py` — `DbtProjectComponent.project` examples

Update the Resolver examples to show the new option.

```python
    project: Annotated[
        DbtProject | DbtProjectManager,
        Resolver(
            resolve_dbt_project,
            model_field_type=str | DbtProjectArgs.model() | RemoteGitDbtProjectManager.model(),
            description="The path to the dbt project or a mapping defining a DbtProject",
            examples=[
                "{{ project_root }}/path/to/dbt_project",
                {
                    "project_dir": "path/to/dbt_project",
                    "profile": "your_profile",
                    "target": "your_target",
                },
+               {
+                   "project_dir": "path/to/dbt_project",
+                   "generate_cli_args": ["compile", "--quiet"],
+               },
            ],
        ),
    ]
```

---

## Summary of what flows where

```
defs.yaml
  └─ generate_cli_args: ["compile", "--quiet"]
       │
       ▼
DbtProjectArgs.generate_cli_args          (resolved from YAML)
       │
       ▼
DbtProjectArgsManager.get_project()       (forwards via **kwargs)
       │
       ▼
DbtProject.__new__(generate_cli_args=...) (new parameter)
       │
       ▼
DagsterDbtProjectPreparer(generate_cli_args=...)  (already supports it)
       │
       ▼
dbt compile --quiet                       (instead of dbt parse --quiet)
```

## What this enables

With this change, adding `compiled_code` to asset descriptions no longer
requires overriding `write_state_to_path`.  A subclass only needs
`get_asset_spec`:

```python
class DbtProjectWithCompiledSql(DbtProjectComponent):

    def get_asset_spec(self, manifest, unique_id, project):
        base_spec = super().get_asset_spec(manifest, unique_id, project)
        props = self.get_resource_props(manifest, unique_id)
        compiled_code = props.get("compiled_code")
        if not compiled_code:
            return base_spec
        section = f"#### Compiled SQL:\n```sql\n{compiled_code}\n```"
        desc = base_spec.description or ""
        new_desc = f"{desc}\n\n{section}" if desc else section
        return base_spec.replace_attributes(description=new_desc)
```

Or, if using the `translation` field in YAML, no subclass at all — just set
`generate_cli_args` and use a translation to read `node.compiled_code`.
