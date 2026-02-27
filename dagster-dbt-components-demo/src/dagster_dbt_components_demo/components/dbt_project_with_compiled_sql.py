from collections.abc import Mapping
from pathlib import Path
from typing import Any, Optional

import dagster as dg
from dagster._annotations import public
from dagster_dbt import DbtProjectComponent
from dagster_dbt.core.resource import DbtCliResource
from dagster_dbt.dbt_project import DbtProject


class DbtProjectWithCompiledSql(DbtProjectComponent):
    """A DbtProjectComponent subclass that appends compiled SQL to asset descriptions.

    This component runs ``dbt compile`` after the default ``dbt parse`` step
    so that the manifest includes the fully-resolved SQL for every model.
    It then adds that compiled SQL to each asset's description under a
    **Compiled SQL** heading.
    """

    def write_state_to_path(self, state_path: Path) -> None:
        # The base class runs `dbt parse`, which does not populate
        # compiled_code in the manifest.
        super().write_state_to_path(state_path)

        # Run `dbt compile` on the prepared project so the manifest
        # includes compiled_code.  This reuses the partial_parse cache
        # from the parse step above, so it's fast.
        project = self._project_manager.get_project(state_path)
        DbtCliResource(project_dir=project).cli(
            ["compile", "--quiet"],
            target_path=project.target_path,
        ).wait()

    @public
    def get_asset_spec(
        self,
        manifest: Mapping[str, Any],
        unique_id: str,
        project: Optional[DbtProject],
    ) -> dg.AssetSpec:
        base_spec = super().get_asset_spec(manifest, unique_id, project)
        resource_props = self.get_resource_props(manifest, unique_id)

        compiled_code = resource_props.get("compiled_code")
        if not compiled_code:
            return base_spec

        existing_description = base_spec.description or ""
        compiled_section = f"#### Compiled SQL:\n```sql\n{compiled_code}\n```"

        if existing_description:
            new_description = f"{existing_description}\n\n{compiled_section}"
        else:
            new_description = compiled_section

        return base_spec.replace_attributes(description=new_description)
