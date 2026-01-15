"""Tests for the example profiled assets."""

import tempfile

import pytest
from dagster import materialize

from dagster_profiler.assets.profiled_assets import (
    cpu_intensive_computation,
    data_aggregation,
    data_transformation,
    generate_large_dataset,
    memory_heavy_processing,
    parallel_profiled_tasks,
    profile_results_summary,
)
from dagster_profiler.resources import ProfilingResource


@pytest.fixture
def profiler_resource():
    """Create a ProfilingResource for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield ProfilingResource(
            output_dir=tmpdir,
            enable_memray=False,  # Disable memray for faster tests
            enable_cprofile=False,  # Disable cprofile for faster tests
        )


class TestIndividualAssets:
    """Test individual assets in isolation."""

    def test_generate_large_dataset(self, profiler_resource):
        """Test the generate_large_dataset asset."""
        result = materialize(
            [generate_large_dataset],
            resources={"profiler": profiler_resource},
        )

        assert result.success
        output = result.output_for_node("generate_large_dataset")
        assert "shape" in output
        assert "memory_bytes" in output
        assert "profile" in output
        assert output["profile"]["task_name"] == "generate_large_dataset"

    def test_cpu_intensive_computation(self, profiler_resource):
        """Test the cpu_intensive_computation asset."""
        result = materialize(
            [cpu_intensive_computation],
            resources={"profiler": profiler_resource},
        )

        assert result.success
        output = result.output_for_node("cpu_intensive_computation")
        assert "n_primes" in output
        assert "largest_prime" in output
        assert output["n_primes"] == 10000
        assert "profile" in output

    def test_parallel_profiled_tasks(self, profiler_resource):
        """Test the parallel_profiled_tasks multi-asset."""
        result = materialize(
            [parallel_profiled_tasks],
            resources={"profiler": profiler_resource},
        )

        assert result.success

        output_a = result.output_for_node("parallel_profiled_tasks", "parallel_task_a")
        output_b = result.output_for_node("parallel_profiled_tasks", "parallel_task_b")

        assert "sum" in output_a
        assert "profile" in output_a
        assert output_a["profile"]["task_name"] == "parallel_task_a"

        assert "result" in output_b
        assert "profile" in output_b
        assert output_b["profile"]["task_name"] == "parallel_task_b"


class TestAssetDependencies:
    """Test assets with dependencies."""

    def test_memory_heavy_processing_chain(self, profiler_resource):
        """Test memory_heavy_processing depends on generate_large_dataset."""
        result = materialize(
            [generate_large_dataset, memory_heavy_processing],
            resources={"profiler": profiler_resource},
        )

        assert result.success
        output = result.output_for_node("memory_heavy_processing")
        assert "stats" in output
        assert "profile" in output

    def test_data_transformation_chain(self, profiler_resource):
        """Test data_transformation depends on generate_large_dataset."""
        result = materialize(
            [generate_large_dataset, data_transformation],
            resources={"profiler": profiler_resource},
        )

        assert result.success
        output = result.output_for_node("data_transformation")
        assert "stats" in output
        assert output["stats"]["final_shape"][0] == 50000


class TestFullPipeline:
    """Test the full profiling pipeline."""

    def test_full_pipeline(self, profiler_resource):
        """Test running all assets in the profiling pipeline."""
        result = materialize(
            [
                generate_large_dataset,
                memory_heavy_processing,
                cpu_intensive_computation,
                data_transformation,
                data_aggregation,
                profile_results_summary,
            ],
            resources={"profiler": profiler_resource},
        )

        assert result.success

        # Check the final summary output
        summary_output = result.output_for_node("profile_results_summary")
        assert "report" in summary_output
        assert "aggregated_stats" in summary_output
        assert "DAGSTER PERFORMANCE PROFILING SUMMARY" in summary_output["report"]

    def test_aggregation_collects_profiles(self, profiler_resource):
        """Test that data_aggregation collects profiles from upstream."""
        result = materialize(
            [
                generate_large_dataset,
                memory_heavy_processing,
                cpu_intensive_computation,
                data_transformation,
                data_aggregation,
            ],
            resources={"profiler": profiler_resource},
        )

        assert result.success

        output = result.output_for_node("data_aggregation")
        aggregated = output["aggregated"]

        assert aggregated["task_count"] == 3
        assert "memory_heavy_processing" in aggregated["profiles"]
        assert "cpu_intensive_computation" in aggregated["profiles"]
        assert "data_transformation" in aggregated["profiles"]


class TestProfilingMetrics:
    """Test that profiling metrics are captured correctly."""

    def test_memory_metrics_captured(self, profiler_resource):
        """Test that memory metrics are present in profile results."""
        result = materialize(
            [generate_large_dataset],
            resources={"profiler": profiler_resource},
        )

        assert result.success
        output = result.output_for_node("generate_large_dataset")
        profile = output["profile"]

        assert "memory_peak_mb" in profile
        assert profile["memory_peak_mb"] > 0
        assert "duration_seconds" in profile
        assert profile["duration_seconds"] > 0

    def test_cpu_metrics_captured(self, profiler_resource):
        """Test that CPU metrics are present in profile results."""
        result = materialize(
            [cpu_intensive_computation],
            resources={"profiler": profiler_resource},
        )

        assert result.success
        output = result.output_for_node("cpu_intensive_computation")
        profile = output["profile"]

        assert "cpu_time_user" in profile
        assert "cpu_time_system" in profile
        assert "cpu_time_total" in profile
        # CPU time should be positive for CPU-intensive work
        assert profile["cpu_time_total"] >= 0
