"""Tests for the external asset demo."""

import pytest
from dagster import build_sensor_context, AssetMaterialization, AssetCheckResult, AssetCheckEvaluation

from external_asset_demo.defs.external_assets import (
    external_data_sensor,
    external_data_checks,
    MockExternalAPIClient,
    external_asset_specs,
)


class TestMockAPIClient:
    """Tests for the MockExternalAPIClient."""

    def test_check_for_new_data_returns_data(self):
        """Test that the mock API returns data in demo mode."""
        client = MockExternalAPIClient(demo_mode=True)
        # Run multiple times to account for randomness
        results = [client.check_for_new_data() for _ in range(10)]
        # At least some should return data (70% chance each time)
        assert any(r is not None for r in results)

    def test_check_for_new_data_structure(self):
        """Test the structure of returned data."""
        client = MockExternalAPIClient(demo_mode=True)
        # Force a result by trying multiple times
        result = None
        for _ in range(20):
            result = client.check_for_new_data()
            if result is not None:
                break

        if result is not None:
            assert "run_id" in result
            assert "timestamp" in result
            assert "rows_processed" in result
            assert "source_system" in result
            assert "status" in result

    def test_validate_data_quality(self):
        """Test data quality validation returns expected structure."""
        client = MockExternalAPIClient(demo_mode=True)
        result = client.validate_data_quality()

        assert "check_passed" in result
        assert "null_count" in result
        assert "duplicate_count" in result
        assert "schema_valid" in result
        assert "freshness_hours" in result


class TestExternalAssetSpecs:
    """Tests for external asset specifications."""

    def test_asset_specs_count(self):
        """Test we have the expected number of asset specs."""
        assert len(external_asset_specs) == 2

    def test_sales_data_spec(self):
        """Test sales data asset spec configuration."""
        sales_spec = next(
            s for s in external_asset_specs
            if "sales_data" in str(s.key)
        )
        assert sales_spec.group_name == "external_data"
        assert sales_spec.metadata is not None

    def test_customer_data_has_dependency(self):
        """Test customer data depends on sales data."""
        customer_spec = next(
            s for s in external_asset_specs
            if "customer_data" in str(s.key)
        )
        assert len(customer_spec.deps) == 1


class TestExternalDataSensor:
    """Tests for the external data sensor."""

    def test_sensor_returns_skip_or_result(self):
        """Test that the sensor returns either SkipReason or SensorResult."""
        from dagster import SkipReason, SensorResult

        context = build_sensor_context()
        result = external_data_sensor(context)

        assert isinstance(result, (SkipReason, SensorResult))

    def test_sensor_result_has_events(self):
        """Test that when sensor returns SensorResult, it has events."""
        from dagster import SensorResult

        # Run multiple times to get a SensorResult
        for _ in range(20):
            context = build_sensor_context()
            result = external_data_sensor(context)
            if isinstance(result, SensorResult):
                assert result.asset_events is not None
                assert len(result.asset_events) > 0
                break

    def test_sensor_materializations(self):
        """Test sensor generates correct materializations."""
        from dagster import SensorResult

        for _ in range(20):
            context = build_sensor_context()
            result = external_data_sensor(context)
            if isinstance(result, SensorResult):
                materializations = [
                    e for e in result.asset_events
                    if isinstance(e, AssetMaterialization)
                ]
                assert len(materializations) == 2  # sales_data and customer_data
                break

    def test_sensor_check_results(self):
        """Test sensor generates asset check evaluations."""
        from dagster import SensorResult

        for _ in range(20):
            context = build_sensor_context()
            result = external_data_sensor(context)
            if isinstance(result, SensorResult):
                check_evals = [
                    e for e in result.asset_events
                    if isinstance(e, AssetCheckEvaluation)
                ]
                assert len(check_evals) == 3  # quality, freshness, completeness
                break


class TestExternalDataChecks:
    """Tests for the external data checks."""

    def test_checks_return_results(self):
        """Test that checks return AssetCheckResult objects."""
        results = list(external_data_checks())
        assert len(results) == 3
        assert all(isinstance(r, AssetCheckResult) for r in results)

    def test_check_results_have_metadata(self):
        """Test that check results include metadata."""
        results = list(external_data_checks())
        for result in results:
            assert result.metadata is not None

    def test_checks_cover_all_assets(self):
        """Test that checks cover both assets."""
        results = list(external_data_checks())
        asset_keys = {str(r.asset_key) for r in results}
        assert len(asset_keys) == 2  # sales_data and customer_data
