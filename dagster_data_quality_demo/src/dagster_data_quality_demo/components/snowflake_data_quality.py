"""Snowflake Data Quality Component for Dagster.

This component executes SQL-based data quality validation queries on Snowflake.
It supports configurable quality rules and both real and demo mode execution.
"""

from collections.abc import Sequence
from dataclasses import dataclass

import dagster as dg
from dagster.components import (
    Component,
    ComponentLoadContext,
    Resolvable,
)
from pydantic import BaseModel


class DataQualityRule(BaseModel):
    """Configuration for a SQL-based data quality rule."""

    name: str
    description: str
    validation_query: str  # SQL query that returns count of violations
    severity: str = "error"  # "error", "warning", "info"
    threshold: int = 0  # Max allowed violations before failure


@dataclass
class SnowflakeDataQualityComponent(Component, Resolvable):
    """Component that executes SQL data quality checks on Snowflake.

    This component creates assets that run configurable data quality rules
    against Snowflake tables and produce validation reports.

    Attributes:
        snowflake_account: Snowflake account identifier
        snowflake_user_env_var: Environment variable for Snowflake username
        snowflake_password_env_var: Environment variable for Snowflake password
        snowflake_warehouse: Snowflake warehouse to use
        snowflake_database: Snowflake database
        snowflake_schema: Snowflake schema
        asset_key: Key for the output validation report asset
        upstream_asset_keys: List of upstream asset keys being validated
        rules: List of data quality rules to execute
        demo_mode: If True, simulates Snowflake queries with mock data
    """

    snowflake_account: str
    snowflake_user_env_var: str
    snowflake_password_env_var: str
    snowflake_warehouse: str
    snowflake_database: str
    snowflake_schema: str
    asset_key: Sequence[str]
    upstream_asset_keys: Sequence[Sequence[str]]
    rules: Sequence[DataQualityRule]
    demo_mode: bool = False

    @classmethod
    def get_component_name(cls) -> str:
        return "snowflake_data_quality"

    @classmethod
    def get_description(cls) -> str:
        return "Executes SQL-based data quality validation rules on Snowflake"

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        """Build Dagster definitions for this component."""

        upstream_deps = [dg.AssetKey(key) for key in self.upstream_asset_keys]
        rules = self.rules
        demo_mode = self.demo_mode
        snowflake_account = self.snowflake_account
        snowflake_user_env_var = self.snowflake_user_env_var
        snowflake_password_env_var = self.snowflake_password_env_var
        snowflake_warehouse = self.snowflake_warehouse
        snowflake_database = self.snowflake_database
        snowflake_schema = self.snowflake_schema
        asset_key = self.asset_key

        @dg.asset(
            key=dg.AssetKey(asset_key),
            deps=upstream_deps,
            description=f"Snowflake data quality validation for {snowflake_database}.{snowflake_schema}",
            metadata={
                "snowflake_account": snowflake_account,
                "snowflake_database": snowflake_database,
                "snowflake_schema": snowflake_schema,
                "rules_count": len(rules),
                "demo_mode": demo_mode,
            },
            kinds={"snowflake", "data_quality"},
        )
        def snowflake_quality_validation(
            context: dg.AssetExecutionContext,
        ) -> dg.MaterializeResult:
            """Execute data quality rules against Snowflake."""

            if demo_mode:
                return _execute_demo_mode(context, rules, snowflake_database, snowflake_schema)
            else:
                return _execute_production_mode(
                    context,
                    rules,
                    snowflake_account,
                    snowflake_user_env_var,
                    snowflake_password_env_var,
                    snowflake_warehouse,
                    snowflake_database,
                    snowflake_schema,
                )

        return dg.Definitions(assets=[snowflake_quality_validation])


def _execute_demo_mode(
    context: dg.AssetExecutionContext,
    rules: Sequence[DataQualityRule],
    database: str,
    schema: str,
) -> dg.MaterializeResult:
    """Execute in demo mode with simulated results."""
    import random

    context.log.info("Running in DEMO MODE - simulating Snowflake data quality checks")

    rule_results = []
    errors = 0
    warnings = 0
    passed = 0

    for rule in rules:
        violations = 0 if random.random() > 0.15 else random.randint(1, 50)
        rule_passed = violations <= rule.threshold

        result = {
            "rule_name": rule.name,
            "description": rule.description,
            "severity": rule.severity,
            "violations_found": violations,
            "threshold": rule.threshold,
            "passed": rule_passed,
            "query": rule.validation_query[:100] + "..."
            if len(rule.validation_query) > 100
            else rule.validation_query,
        }
        rule_results.append(result)

        if rule_passed:
            passed += 1
        elif rule.severity == "error":
            errors += 1
        else:
            warnings += 1

    overall_passed = errors == 0

    context.log.info(
        f"Data quality validation completed: {passed} passed, {errors} errors, {warnings} warnings"
    )

    return dg.MaterializeResult(
        metadata={
            "demo_mode": True,
            "overall_passed": overall_passed,
            "rules_passed": passed,
            "rules_with_errors": errors,
            "rules_with_warnings": warnings,
            "rule_results": rule_results,
            "snowflake_location": f"{database}.{schema}",
        }
    )


def _execute_production_mode(
    context: dg.AssetExecutionContext,
    rules: Sequence[DataQualityRule],
    snowflake_account: str,
    snowflake_user_env_var: str,
    snowflake_password_env_var: str,
    snowflake_warehouse: str,
    snowflake_database: str,
    snowflake_schema: str,
) -> dg.MaterializeResult:
    """Execute real Snowflake queries."""
    import os

    import snowflake.connector

    context.log.info(
        f"Connecting to Snowflake: {snowflake_account}, "
        f"database: {snowflake_database}, schema: {snowflake_schema}"
    )

    user = os.environ.get(snowflake_user_env_var)
    password = os.environ.get(snowflake_password_env_var)

    if not user or not password:
        raise ValueError(
            f"Snowflake credentials not found. Check environment variables: "
            f"{snowflake_user_env_var}, {snowflake_password_env_var}"
        )

    conn = snowflake.connector.connect(
        account=snowflake_account,
        user=user,
        password=password,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema,
    )

    rule_results = []
    errors = 0
    warnings = 0
    passed = 0

    try:
        cursor = conn.cursor()

        for rule in rules:
            context.log.info(f"Executing rule: {rule.name}")

            try:
                cursor.execute(rule.validation_query)
                result_row = cursor.fetchone()
                violations = result_row[0] if result_row else 0
            except Exception as e:
                context.log.error(f"Error executing rule {rule.name}: {e}")
                violations = -1

            rule_passed = violations <= rule.threshold and violations >= 0

            result = {
                "rule_name": rule.name,
                "description": rule.description,
                "severity": rule.severity,
                "violations_found": violations,
                "threshold": rule.threshold,
                "passed": rule_passed,
                "query": rule.validation_query[:100] + "..."
                if len(rule.validation_query) > 100
                else rule.validation_query,
            }
            rule_results.append(result)

            if rule_passed:
                passed += 1
            elif rule.severity == "error":
                errors += 1
            else:
                warnings += 1

    finally:
        conn.close()

    overall_passed = errors == 0

    context.log.info(
        f"Data quality validation completed: {passed} passed, {errors} errors, {warnings} warnings"
    )

    return dg.MaterializeResult(
        metadata={
            "demo_mode": False,
            "overall_passed": overall_passed,
            "rules_passed": passed,
            "rules_with_errors": errors,
            "rules_with_warnings": warnings,
            "rule_results": rule_results,
            "snowflake_location": f"{snowflake_database}.{snowflake_schema}",
        }
    )
