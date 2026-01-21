"""Azure Function Data Quality Component for Dagster.

This component invokes Azure Functions to perform data quality checks on upstream data assets.
It supports both real Azure Function invocations and a demo mode for local testing.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import dagster as dg
import requests
from dagster.components import (
    Component,
    ComponentLoadContext,
    Resolvable,
)
from pydantic import BaseModel


class DataQualityCheck(BaseModel):
    """Configuration for a single data quality check."""

    name: str
    description: str
    check_type: str  # e.g., "null_check", "range_check", "uniqueness", "freshness"
    parameters: dict[str, Any] | None = None


@dataclass
class AzureFunctionDataQualityComponent(Component, Resolvable):
    """Component that invokes Azure Functions for data quality validation.

    This component creates assets that run data quality checks by calling Azure Functions.
    Each check validates upstream data and produces a quality report asset.

    Attributes:
        function_app_url: Base URL of the Azure Function App
        function_name: Name of the data quality function to invoke
        function_key_env_var: Environment variable containing the function key
        asset_key: Key for the output quality report asset
        upstream_asset_keys: List of upstream asset keys to validate
        checks: List of data quality checks to perform
        demo_mode: If True, simulates Azure Function calls with mock data
    """

    function_app_url: str
    function_name: str
    function_key_env_var: str
    asset_key: Sequence[str]
    upstream_asset_keys: Sequence[Sequence[str]]
    checks: Sequence[DataQualityCheck]
    demo_mode: bool = False

    @classmethod
    def get_component_name(cls) -> str:
        return "azure_function_data_quality"

    @classmethod
    def get_description(cls) -> str:
        return "Invokes Azure Functions to perform data quality checks on upstream data assets"

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        """Build Dagster definitions for this component."""

        upstream_deps = [dg.AssetKey(key) for key in self.upstream_asset_keys]
        checks = self.checks
        function_app_url = self.function_app_url
        function_name = self.function_name
        function_key_env_var = self.function_key_env_var
        demo_mode = self.demo_mode
        asset_key = self.asset_key

        @dg.asset(
            key=dg.AssetKey(asset_key),
            deps=upstream_deps,
            description=f"Data quality report from Azure Function: {function_name}",
            metadata={
                "function_app_url": function_app_url,
                "function_name": function_name,
                "checks_count": len(checks),
                "demo_mode": demo_mode,
            },
            kinds={"azure", "data_quality"},
        )
        def data_quality_report(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            """Execute data quality checks via Azure Function."""

            if demo_mode:
                return _execute_demo_mode(context, checks)
            else:
                return _execute_production_mode(
                    context, function_app_url, function_name, function_key_env_var, checks
                )

        return dg.Definitions(assets=[data_quality_report])


def _execute_demo_mode(
    context: dg.AssetExecutionContext, checks: Sequence[DataQualityCheck]
) -> dg.MaterializeResult:
    """Execute in demo mode with simulated results."""
    import random

    context.log.info("Running in DEMO MODE - simulating Azure Function call")

    check_results = []
    total_passed = 0
    total_failed = 0

    for check in checks:
        passed = random.random() > 0.1  # 90% pass rate in demo
        records_checked = random.randint(10000, 100000)
        failed_records = 0 if passed else random.randint(1, 100)

        result = {
            "check_name": check.name,
            "check_type": check.check_type,
            "description": check.description,
            "passed": passed,
            "records_checked": records_checked,
            "failed_records": failed_records,
            "parameters": check.parameters or {},
        }
        check_results.append(result)

        if passed:
            total_passed += 1
        else:
            total_failed += 1

    overall_passed = total_failed == 0

    context.log.info(
        f"Data quality checks completed: {total_passed} passed, {total_failed} failed"
    )

    return dg.MaterializeResult(
        metadata={
            "demo_mode": True,
            "overall_passed": overall_passed,
            "checks_passed": total_passed,
            "checks_failed": total_failed,
            "check_results": check_results,
        }
    )


def _execute_production_mode(
    context: dg.AssetExecutionContext,
    function_app_url: str,
    function_name: str,
    function_key_env_var: str,
    checks: Sequence[DataQualityCheck],
) -> dg.MaterializeResult:
    """Execute real Azure Function call."""
    import os

    context.log.info(f"Invoking Azure Function: {function_app_url}/api/{function_name}")

    function_key = os.environ.get(function_key_env_var)
    if not function_key:
        raise ValueError(
            f"Function key not found in environment variable: {function_key_env_var}"
        )

    payload = {
        "checks": [
            {
                "name": check.name,
                "check_type": check.check_type,
                "description": check.description,
                "parameters": check.parameters or {},
            }
            for check in checks
        ],
    }

    url = f"{function_app_url}/api/{function_name}"
    headers = {
        "Content-Type": "application/json",
        "x-functions-key": function_key,
    }

    response = requests.post(url, json=payload, headers=headers, timeout=300)
    response.raise_for_status()

    result = response.json()

    context.log.info(
        f"Azure Function response: {result.get('checks_passed', 0)} checks passed, "
        f"{result.get('checks_failed', 0)} checks failed"
    )

    return dg.MaterializeResult(
        metadata={
            "demo_mode": False,
            "overall_passed": result.get("overall_passed", False),
            "checks_passed": result.get("checks_passed", 0),
            "checks_failed": result.get("checks_failed", 0),
            "check_results": result.get("check_results", []),
            "function_url": url,
        }
    )
