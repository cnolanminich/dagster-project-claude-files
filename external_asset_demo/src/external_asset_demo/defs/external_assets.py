"""External Asset Demo with Sensor, Asset Specs, and Asset Check Specs.

This module demonstrates:
1. External assets using AssetSpec (assets managed outside Dagster)
2. Asset checks using @multi_asset_check decorator
3. A sensor that polls a mock API and reports materializations and check results
"""

import random
from datetime import datetime
from typing import Any, Iterator

from dagster import (
    AssetCheckEvaluation,
    AssetCheckKey,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetKey,
    AssetMaterialization,
    AssetSpec,
    Definitions,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    multi_asset_check,
    sensor,
)


# =============================================================================
# Mock API Client
# =============================================================================


class MockExternalAPIClient:
    """A mock API client that simulates an external data pipeline system.

    In a real scenario, this would connect to an external system like:
    - An Airflow instance
    - A Spark job tracker
    - An external ETL tool
    - A data warehouse job scheduler
    """

    def __init__(self, demo_mode: bool = True):
        self.demo_mode = demo_mode
        self._last_run_id = 0

    def check_for_new_data(self) -> dict[str, Any] | None:
        """Check if new data has been processed by the external system.

        Returns:
            A dict with run info if new data is available, None otherwise.
        """
        if self.demo_mode:
            # In demo mode, simulate new data arriving ~70% of the time
            if random.random() < 0.7:
                self._last_run_id += 1
                return {
                    "run_id": f"external_run_{self._last_run_id}",
                    "timestamp": datetime.now().isoformat(),
                    "rows_processed": random.randint(1000, 100000),
                    "source_system": "external_etl_pipeline",
                    "status": "completed",
                }
        return None

    def validate_data_quality(self) -> dict[str, Any]:
        """Run data quality checks on the externally processed data.

        Returns:
            A dict with validation results including pass/fail status.
        """
        if self.demo_mode:
            # Randomly pass or fail the data quality check
            passed = random.choice([True, True, True, False])  # 75% pass rate

            return {
                "check_passed": passed,
                "null_count": random.randint(0, 100) if not passed else 0,
                "duplicate_count": random.randint(0, 50) if not passed else 0,
                "schema_valid": passed,
                "freshness_hours": random.uniform(0.1, 2.0) if passed else random.uniform(5.0, 24.0),
                "error_message": "Data quality issues detected" if not passed else None,
            }

        # In production mode, you would call a real API
        return {"check_passed": True, "null_count": 0, "duplicate_count": 0, "schema_valid": True}


# =============================================================================
# External Asset Definitions (using AssetSpec)
# =============================================================================

# Define external assets using AssetSpec
# These represent data assets managed outside of Dagster

external_sales_data = AssetSpec(
    key=AssetKey(["external", "sales_data"]),
    description="Sales transaction data processed by an external ETL pipeline",
    group_name="external_data",
    metadata={
        "source_system": "external_etl_pipeline",
        "update_frequency": "hourly",
        "owner": "data-platform-team",
    },
)

external_customer_data = AssetSpec(
    key=AssetKey(["external", "customer_data"]),
    description="Customer master data synchronized from CRM system",
    group_name="external_data",
    deps=[AssetKey(["external", "sales_data"])],
    metadata={
        "source_system": "salesforce_sync",
        "update_frequency": "daily",
        "owner": "crm-team",
    },
)

# Collect all external asset specs
external_asset_specs = [external_sales_data, external_customer_data]


# =============================================================================
# Asset Check Definitions (using @multi_asset_check)
# =============================================================================

# Define asset check specs
sales_data_quality_check_spec = AssetCheckSpec(
    name="data_quality_check",
    asset=AssetKey(["external", "sales_data"]),
    description="Validates data quality of externally processed sales data",
)

sales_data_freshness_check_spec = AssetCheckSpec(
    name="freshness_check",
    asset=AssetKey(["external", "sales_data"]),
    description="Ensures sales data is fresh and up-to-date",
)

customer_data_completeness_check_spec = AssetCheckSpec(
    name="completeness_check",
    asset=AssetKey(["external", "customer_data"]),
    description="Validates customer data has all required fields",
)


@multi_asset_check(
    specs=[
        sales_data_quality_check_spec,
        sales_data_freshness_check_spec,
        customer_data_completeness_check_spec,
    ],
)
def external_data_checks() -> Iterator[AssetCheckResult]:
    """Asset checks for external data assets.

    These checks validate data managed by external systems.
    In demo mode, they use the mock API client to simulate validation.
    The sensor can also report check results when polling external systems.
    """
    api_client = MockExternalAPIClient(demo_mode=True)
    validation_result = api_client.validate_data_quality()

    # Sales data quality check
    yield AssetCheckResult(
        asset_key=AssetKey(["external", "sales_data"]),
        check_name="data_quality_check",
        passed=validation_result["check_passed"],
        metadata={
            "null_count": validation_result["null_count"],
            "duplicate_count": validation_result["duplicate_count"],
            "schema_valid": validation_result["schema_valid"],
        },
        severity=AssetCheckSeverity.ERROR if not validation_result["check_passed"] else AssetCheckSeverity.WARN,
    )

    # Sales data freshness check
    freshness_passed = validation_result["freshness_hours"] < 3.0
    yield AssetCheckResult(
        asset_key=AssetKey(["external", "sales_data"]),
        check_name="freshness_check",
        passed=freshness_passed,
        metadata={
            "freshness_hours": round(validation_result["freshness_hours"], 2),
            "threshold_hours": 3.0,
        },
        severity=AssetCheckSeverity.WARN,
    )

    # Customer data completeness check
    customer_check_passed = random.choice([True, True, False])
    yield AssetCheckResult(
        asset_key=AssetKey(["external", "customer_data"]),
        check_name="completeness_check",
        passed=customer_check_passed,
        metadata={
            "missing_fields": [] if customer_check_passed else ["phone_number", "email"],
            "completeness_ratio": 1.0 if customer_check_passed else 0.85,
        },
        severity=AssetCheckSeverity.WARN,
    )


# =============================================================================
# Sensor Definition
# =============================================================================

# Initialize the mock API client for the sensor
_api_client = MockExternalAPIClient(demo_mode=True)


@sensor(
    name="external_data_sensor",
    description="Monitors external ETL pipeline and reports materializations and check results",
    minimum_interval_seconds=30,
)
def external_data_sensor(context: SensorEvaluationContext) -> SensorResult | SkipReason:
    """Sensor that polls an external API and reports asset materializations and check results.

    This sensor:
    1. Checks if new data has been processed by the external system
    2. If so, yields an AssetMaterialization event
    3. Runs data quality validation and yields AssetCheckResult (pass or fail randomly)
    """
    # Check for new data from the external system
    new_data = _api_client.check_for_new_data()

    if new_data is None:
        return SkipReason("No new data detected from external system")

    context.log.info(f"New data detected: {new_data['run_id']}")

    # Prepare events to yield
    asset_events = []

    # Create asset materialization for sales data
    sales_materialization = AssetMaterialization(
        asset_key=AssetKey(["external", "sales_data"]),
        metadata={
            "run_id": new_data["run_id"],
            "timestamp": new_data["timestamp"],
            "rows_processed": new_data["rows_processed"],
            "source_system": new_data["source_system"],
        },
        description=f"External sales data updated via {new_data['source_system']}",
    )
    asset_events.append(sales_materialization)

    # Create asset materialization for customer data (dependent on sales)
    customer_materialization = AssetMaterialization(
        asset_key=AssetKey(["external", "customer_data"]),
        metadata={
            "run_id": new_data["run_id"],
            "timestamp": new_data["timestamp"],
            "source_system": "salesforce_sync",
        },
        description="Customer data synchronized after sales data update",
    )
    asset_events.append(customer_materialization)

    # Run data quality validation
    validation_result = _api_client.validate_data_quality()

    # Create asset check evaluations based on validation
    # Note: Sensors use AssetCheckEvaluation, not AssetCheckResult

    # Sales data quality check
    sales_quality_eval = AssetCheckEvaluation(
        asset_key=AssetKey(["external", "sales_data"]),
        check_name="data_quality_check",
        passed=validation_result["check_passed"],
        metadata={
            "null_count": validation_result["null_count"],
            "duplicate_count": validation_result["duplicate_count"],
            "schema_valid": validation_result["schema_valid"],
        },
        severity=AssetCheckSeverity.ERROR if not validation_result["check_passed"] else AssetCheckSeverity.WARN,
    )
    asset_events.append(sales_quality_eval)

    # Sales data freshness check (based on freshness_hours)
    freshness_passed = validation_result["freshness_hours"] < 3.0
    sales_freshness_eval = AssetCheckEvaluation(
        asset_key=AssetKey(["external", "sales_data"]),
        check_name="freshness_check",
        passed=freshness_passed,
        metadata={
            "freshness_hours": round(validation_result["freshness_hours"], 2),
            "threshold_hours": 3.0,
        },
        severity=AssetCheckSeverity.WARN,
    )
    asset_events.append(sales_freshness_eval)

    # Customer data completeness check (random for demo)
    customer_check_passed = random.choice([True, True, False])
    customer_completeness_eval = AssetCheckEvaluation(
        asset_key=AssetKey(["external", "customer_data"]),
        check_name="completeness_check",
        passed=customer_check_passed,
        metadata={
            "missing_fields": [] if customer_check_passed else ["phone_number", "email"],
            "completeness_ratio": 1.0 if customer_check_passed else 0.85,
        },
        severity=AssetCheckSeverity.WARN,
    )
    asset_events.append(customer_completeness_eval)

    context.log.info(
        f"Reporting materialization for run {new_data['run_id']} with "
        f"quality check: {'PASSED' if validation_result['check_passed'] else 'FAILED'}"
    )

    return SensorResult(
        asset_events=asset_events,
    )


# =============================================================================
# Definitions Export
# =============================================================================

defs = Definitions(
    assets=external_asset_specs,
    asset_checks=[external_data_checks],
    sensors=[external_data_sensor],
)
