"""Tests for profiling utilities."""

import os
import tempfile
import time

import numpy as np
import pytest

from dagster_profiler.profiling import (
    CPUProfiler,
    MemoryProfiler,
    ProfileResult,
    profile_cpu,
    profile_memory,
)
from dagster_profiler.profiling.memory_profiler import MEMRAY_AVAILABLE


class TestProfileResult:
    """Tests for ProfileResult dataclass."""

    def test_profile_result_creation(self):
        """Test creating a ProfileResult."""
        from datetime import datetime

        result = ProfileResult(
            task_name="test_task",
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_seconds=1.5,
            memory_peak_bytes=1024 * 1024 * 100,  # 100 MB
            cpu_time_user=0.5,
            cpu_time_system=0.1,
        )

        assert result.task_name == "test_task"
        assert result.duration_seconds == 1.5
        assert result.memory_peak_mb == pytest.approx(100.0, rel=0.01)
        assert result.cpu_time_total == pytest.approx(0.6, rel=0.01)

    def test_to_dict(self):
        """Test converting ProfileResult to dictionary."""
        from datetime import datetime

        result = ProfileResult(
            task_name="test",
            start_time=datetime(2024, 1, 1, 12, 0, 0),
            end_time=datetime(2024, 1, 1, 12, 0, 1),
            duration_seconds=1.0,
        )

        d = result.to_dict()
        assert d["task_name"] == "test"
        assert d["duration_seconds"] == 1.0
        assert "start_time" in d
        assert "end_time" in d

    def test_summary(self):
        """Test generating summary string."""
        from datetime import datetime

        result = ProfileResult(
            task_name="test",
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_seconds=2.5,
            memory_peak_bytes=50 * 1024 * 1024,
            cpu_time_user=1.0,
            cpu_time_system=0.5,
            cpu_percent_avg=50.0,
            cpu_percent_peak=80.0,
        )

        summary = result.summary()
        assert "test" in summary
        assert "2.5" in summary
        assert "Memory:" in summary
        assert "CPU:" in summary


class TestMemoryProfiler:
    """Tests for MemoryProfiler."""

    def test_memory_profiler_basic(self):
        """Test basic memory profiling."""
        with tempfile.TemporaryDirectory() as tmpdir:
            profiler = MemoryProfiler(output_dir=tmpdir, use_memray=False)

            with profiler.profile("test_task") as result:
                # Allocate some memory
                data = [i for i in range(100000)]
                _ = np.zeros(1000000)

            assert result.task_name == "test_task"
            assert result.duration_seconds > 0
            assert result.memory_peak_bytes > 0
            assert result.memory_start_bytes > 0

    def test_memory_profiler_with_memray(self):
        """Test memory profiling with memray (if available)."""
        if not MEMRAY_AVAILABLE:
            pytest.skip("memray not available")

        with tempfile.TemporaryDirectory() as tmpdir:
            profiler = MemoryProfiler(output_dir=tmpdir, use_memray=True)

            with profiler.profile("memray_test") as result:
                data = np.random.randn(100000)
                _ = data * 2

            assert result.memray_output_path is not None
            assert os.path.exists(result.memray_output_path)

    def test_memory_decorator(self):
        """Test the profile_memory decorator."""

        @profile_memory(task_name="decorated_func", use_memray=False)
        def allocate_memory():
            return [i ** 2 for i in range(10000)]

        result_value, profile_result = allocate_memory()

        assert len(result_value) == 10000
        assert profile_result.task_name == "decorated_func"
        assert profile_result.duration_seconds > 0
        assert profile_result.memory_peak_bytes > 0


class TestCPUProfiler:
    """Tests for CPUProfiler."""

    def test_cpu_profiler_basic(self):
        """Test basic CPU profiling."""
        with tempfile.TemporaryDirectory() as tmpdir:
            profiler = CPUProfiler(output_dir=tmpdir, enable_cprofile=True)

            with profiler.profile("cpu_test") as result:
                # Do some CPU work
                total = sum(i ** 2 for i in range(10000))

            assert result.task_name == "cpu_test"
            assert result.duration_seconds > 0
            assert result.cpu_time_user >= 0
            assert result.cpu_time_system >= 0

    def test_cpu_profiler_cprofile_output(self):
        """Test that cProfile output is saved."""
        with tempfile.TemporaryDirectory() as tmpdir:
            profiler = CPUProfiler(output_dir=tmpdir, enable_cprofile=True)

            with profiler.profile("cprofile_test") as result:
                # CPU-intensive operation
                for _ in range(100):
                    _ = sum(i * i for i in range(1000))

            assert result.cprofile_output_path is not None
            assert os.path.exists(result.cprofile_output_path)

    def test_cpu_profiler_stats(self):
        """Test getting cProfile stats."""
        with tempfile.TemporaryDirectory() as tmpdir:
            profiler = CPUProfiler(output_dir=tmpdir, enable_cprofile=True)

            with profiler.profile("stats_test") as result:
                for _ in range(50):
                    _ = [i ** 2 for i in range(100)]

            stats = profiler.get_stats_string(result.cprofile_output_path, limit=10)
            assert len(stats) > 0
            assert "function calls" in stats.lower() or "cumulative" in stats.lower()

    def test_cpu_decorator(self):
        """Test the profile_cpu decorator."""

        @profile_cpu(task_name="cpu_decorated")
        def cpu_work():
            return sum(i ** 2 for i in range(10000))

        result_value, profile_result = cpu_work()

        assert result_value > 0
        assert profile_result.task_name == "cpu_decorated"
        assert profile_result.duration_seconds > 0

    def test_cpu_monitor_samples(self):
        """Test CPU monitoring collects samples."""
        from dagster_profiler.profiling.cpu_profiler import CPUMonitor

        monitor = CPUMonitor(interval=0.05)
        monitor.start()

        # Do some work while monitoring
        start = time.time()
        while time.time() - start < 0.3:
            _ = sum(i ** 2 for i in range(1000))

        monitor.stop()

        assert len(monitor.samples) > 0
        assert monitor.average >= 0
        assert monitor.peak >= 0


class TestCombinedProfiling:
    """Tests for combined memory and CPU profiling."""

    def test_sequential_profiling(self):
        """Test running memory and CPU profiling sequentially."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mem_profiler = MemoryProfiler(output_dir=tmpdir, use_memray=False)
            cpu_profiler = CPUProfiler(output_dir=tmpdir, enable_cprofile=False)

            # Memory profiling
            with mem_profiler.profile("memory_phase") as mem_result:
                data = np.random.randn(100000)

            # CPU profiling
            with cpu_profiler.profile("cpu_phase") as cpu_result:
                _ = sum(i ** 2 for i in range(10000))

            assert mem_result.memory_peak_bytes > 0
            assert cpu_result.cpu_time_user >= 0

    def test_mixed_workload(self):
        """Test profiling a mixed memory/CPU workload."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mem_profiler = MemoryProfiler(output_dir=tmpdir, use_memray=False)

            with mem_profiler.profile("mixed_workload") as result:
                # Memory allocation
                data = np.random.randn(50000)
                # CPU work on the data
                processed = np.sqrt(np.abs(data)) * np.log1p(np.abs(data))
                # More memory
                result_data = processed.tolist()

            assert result.duration_seconds > 0
            assert result.memory_peak_bytes > 0
