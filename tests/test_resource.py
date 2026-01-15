"""Tests for the ProfilingResource Dagster integration."""

import os
import tempfile

import numpy as np
import pytest
from dagster import asset, materialize, AssetExecutionContext

from dagster_profiler.resources import ProfilingResource


class TestProfilingResource:
    """Tests for ProfilingResource."""

    def test_resource_creation(self):
        """Test creating a ProfilingResource."""
        with tempfile.TemporaryDirectory() as tmpdir:
            resource = ProfilingResource(
                output_dir=tmpdir,
                enable_memray=False,
                enable_cprofile=True,
            )

            assert resource.output_dir == tmpdir
            assert resource.enable_memray is False
            assert resource.enable_cprofile is True

    def test_resource_profile_context_manager(self):
        """Test using the profile context manager."""
        with tempfile.TemporaryDirectory() as tmpdir:
            resource = ProfilingResource(
                output_dir=tmpdir,
                enable_memray=False,
                enable_cprofile=False,
            )

            with resource.profile("test_task") as result:
                data = [i ** 2 for i in range(10000)]

            assert result.task_name == "test_task"
            assert result.duration_seconds > 0
            assert result.memory_peak_bytes > 0

    def test_resource_profile_memory_only(self):
        """Test memory-only profiling."""
        with tempfile.TemporaryDirectory() as tmpdir:
            resource = ProfilingResource(
                output_dir=tmpdir,
                enable_memray=False,
                enable_cprofile=True,
            )

            with resource.profile_memory_only("memory_test") as result:
                _ = np.zeros(100000)

            assert result.memory_peak_bytes > 0
            # cprofile should not be saved when using memory_only
            assert result.cprofile_output_path is None

    def test_resource_profile_cpu_only(self):
        """Test CPU-only profiling."""
        with tempfile.TemporaryDirectory() as tmpdir:
            resource = ProfilingResource(
                output_dir=tmpdir,
                enable_memray=False,
                enable_cprofile=True,
            )

            with resource.profile_cpu_only("cpu_test") as result:
                _ = sum(i ** 2 for i in range(10000))

            assert result.cpu_time_user >= 0
            assert result.cprofile_output_path is not None
            assert os.path.exists(result.cprofile_output_path)

    def test_resource_with_dagster_asset(self):
        """Test using ProfilingResource with a Dagster asset."""
        with tempfile.TemporaryDirectory() as tmpdir:
            @asset
            def test_asset(profiler: ProfilingResource):
                with profiler.profile("asset_task") as result:
                    data = np.random.randn(10000)
                    processed = np.sqrt(np.abs(data))

                return {
                    "sum": float(np.sum(processed)),
                    "peak_memory_mb": result.memory_peak_mb,
                    "duration": result.duration_seconds,
                }

            resource = ProfilingResource(
                output_dir=tmpdir,
                enable_memray=False,
                enable_cprofile=False,
            )

            result = materialize(
                [test_asset],
                resources={"profiler": resource},
            )

            assert result.success

    def test_multiple_profile_sessions(self):
        """Test running multiple profile sessions sequentially."""
        with tempfile.TemporaryDirectory() as tmpdir:
            resource = ProfilingResource(
                output_dir=tmpdir,
                enable_memray=False,
                enable_cprofile=False,
            )

            results = []
            for i in range(3):
                with resource.profile(f"task_{i}") as result:
                    data = [j ** 2 for j in range(1000 * (i + 1))]
                results.append(result)

            assert len(results) == 3
            for i, r in enumerate(results):
                assert r.task_name == f"task_{i}"
                assert r.duration_seconds > 0


class TestProfilingResourceIntegration:
    """Integration tests for ProfilingResource with Dagster."""

    def test_asset_chain_with_profiling(self):
        """Test a chain of assets using the profiling resource."""
        with tempfile.TemporaryDirectory() as tmpdir:
            @asset
            def first_asset(profiler: ProfilingResource):
                with profiler.profile("first") as result:
                    data = list(range(10000))
                return {"count": len(data), "profile": result.to_dict()}

            @asset
            def second_asset(profiler: ProfilingResource, first_asset):
                with profiler.profile("second") as result:
                    doubled = first_asset["count"] * 2
                return {"doubled": doubled, "profile": result.to_dict()}

            resource = ProfilingResource(
                output_dir=tmpdir,
                enable_memray=False,
                enable_cprofile=False,
            )

            result = materialize(
                [first_asset, second_asset],
                resources={"profiler": resource},
            )

            assert result.success

    def test_profiler_metadata_output(self):
        """Test that profiling metadata is captured correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            @asset
            def metadata_asset(
                context: AssetExecutionContext,
                profiler: ProfilingResource,
            ):
                with profiler.profile("metadata_test") as result:
                    # Allocate some memory
                    data = np.random.randn(50000)
                    # Do some computation
                    _ = np.sum(data ** 2)

                profiler.log_result(result)
                return result.to_dict()

            resource = ProfilingResource(
                output_dir=tmpdir,
                enable_memray=False,
                enable_cprofile=False,
            )

            mat_result = materialize(
                [metadata_asset],
                resources={"profiler": resource},
            )

            assert mat_result.success
