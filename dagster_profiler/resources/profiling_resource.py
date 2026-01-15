"""Dagster resource for integrated profiling of ops and assets."""

import os
import tempfile
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Generator

import psutil
from dagster import ConfigurableResource, get_dagster_logger
from pydantic import Field

from dagster_profiler.profiling.cpu_profiler import CPUMonitor, CPUProfiler
from dagster_profiler.profiling.memory_profiler import MEMRAY_AVAILABLE, MemoryProfiler
from dagster_profiler.profiling.models import ProfileResult

# Try to import memray
try:
    import memray
except ImportError:
    memray = None


class ProfilingResource(ConfigurableResource):
    """A Dagster resource that provides memory and CPU profiling capabilities.

    This resource can be used to profile individual ops, assets, or entire jobs.
    It tracks memory allocations, CPU usage, and generates detailed profiling reports.

    Example usage:
        ```python
        @asset
        def my_asset(profiler: ProfilingResource):
            with profiler.profile("data_processing") as result:
                # Do expensive computation
                process_data()

            profiler.log_result(result)
            return result.to_dict()
        ```
    """

    output_dir: str = Field(
        default="",
        description="Directory to store profiling output files. If empty, uses a temp directory.",
    )
    enable_memray: bool = Field(
        default=True,
        description="Enable memray for detailed memory profiling (if available).",
    )
    enable_cprofile: bool = Field(
        default=True,
        description="Enable cProfile for detailed CPU profiling.",
    )
    track_native_memory: bool = Field(
        default=False,
        description="Track native (C/C++) memory allocations (memray only, slower).",
    )
    cpu_monitor_interval: float = Field(
        default=0.1,
        description="CPU monitoring sample interval in seconds.",
    )

    def _get_output_dir(self) -> str:
        """Get or create the output directory."""
        if self.output_dir:
            os.makedirs(self.output_dir, exist_ok=True)
            return self.output_dir
        return tempfile.mkdtemp(prefix="dagster_profile_")

    @contextmanager
    def profile(
        self,
        task_name: str,
        profile_memory: bool = True,
        profile_cpu: bool = True,
    ) -> Generator[ProfileResult, None, None]:
        """Context manager that profiles both memory and CPU usage.

        Args:
            task_name: Name identifier for this profiling session
            profile_memory: Whether to profile memory
            profile_cpu: Whether to profile CPU

        Yields:
            ProfileResult populated with profiling metrics after execution
        """
        logger = get_dagster_logger()
        output_dir = self._get_output_dir()

        result = ProfileResult(
            task_name=task_name,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_seconds=0.0,
        )

        process = psutil.Process(os.getpid())

        # Initialize profilers
        import tracemalloc

        memray_tracker = None
        cpu_profiler = None
        cpu_monitor = None

        # Memory setup
        if profile_memory:
            result.memory_start_bytes = process.memory_info().rss
            tracemalloc.start()

            if self.enable_memray and MEMRAY_AVAILABLE:
                memray_path = os.path.join(
                    output_dir,
                    f"{task_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.bin",
                )
                result.memray_output_path = memray_path
                memray_tracker = memray.Tracker(
                    memray_path,
                    native_traces=self.track_native_memory,
                )
                memray_tracker.__enter__()
                logger.info(f"Started memray profiling for '{task_name}'")

        # CPU setup
        if profile_cpu:
            cpu_monitor = CPUMonitor(interval=self.cpu_monitor_interval)
            cpu_monitor.start()

            if self.enable_cprofile:
                import cProfile

                cpu_profiler = cProfile.Profile()
                cpu_profiler.enable()

        cpu_times_start = process.cpu_times()

        try:
            yield result
        finally:
            result.end_time = datetime.now()
            result.duration_seconds = (result.end_time - result.start_time).total_seconds()

            # Collect memory metrics
            if profile_memory:
                current, peak = tracemalloc.get_traced_memory()
                tracemalloc.stop()
                result.memory_allocated_bytes = current
                result.memory_peak_bytes = peak
                result.memory_end_bytes = process.memory_info().rss

                if memray_tracker:
                    memray_tracker.__exit__(None, None, None)
                    logger.info(f"Memray output saved to: {result.memray_output_path}")

            # Collect CPU metrics
            cpu_times_end = process.cpu_times()
            result.cpu_time_user = cpu_times_end.user - cpu_times_start.user
            result.cpu_time_system = cpu_times_end.system - cpu_times_start.system

            if profile_cpu:
                if cpu_monitor:
                    cpu_monitor.stop()
                    result.cpu_percent_avg = cpu_monitor.average
                    result.cpu_percent_peak = cpu_monitor.peak

                if cpu_profiler:
                    cpu_profiler.disable()
                    prof_path = os.path.join(
                        output_dir,
                        f"{task_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.prof",
                    )
                    cpu_profiler.dump_stats(prof_path)
                    result.cprofile_output_path = prof_path
                    logger.info(f"cProfile output saved to: {prof_path}")

    @contextmanager
    def profile_memory_only(self, task_name: str) -> Generator[ProfileResult, None, None]:
        """Profile only memory usage.

        Args:
            task_name: Name identifier for this profiling session

        Yields:
            ProfileResult with memory metrics
        """
        with self.profile(task_name, profile_memory=True, profile_cpu=False) as result:
            yield result

    @contextmanager
    def profile_cpu_only(self, task_name: str) -> Generator[ProfileResult, None, None]:
        """Profile only CPU usage.

        Args:
            task_name: Name identifier for this profiling session

        Yields:
            ProfileResult with CPU metrics
        """
        with self.profile(task_name, profile_memory=False, profile_cpu=True) as result:
            yield result

    def log_result(self, result: ProfileResult, level: str = "info") -> None:
        """Log profiling results using Dagster logger.

        Args:
            result: ProfileResult to log
            level: Log level ('debug', 'info', 'warning')
        """
        logger = get_dagster_logger()
        log_fn = getattr(logger, level, logger.info)
        log_fn(result.summary())

    def generate_flamegraph(self, result: ProfileResult) -> str | None:
        """Generate an HTML flamegraph from memray output.

        Args:
            result: ProfileResult with memray_output_path

        Returns:
            Path to generated HTML file, or None if generation failed
        """
        if not result.memray_output_path or not MEMRAY_AVAILABLE:
            return None

        profiler = MemoryProfiler(output_dir=os.path.dirname(result.memray_output_path))
        return profiler.generate_report(result.memray_output_path, "html")

    def get_cpu_stats(self, result: ProfileResult, limit: int = 20) -> str:
        """Get formatted cProfile statistics.

        Args:
            result: ProfileResult with cprofile_output_path
            limit: Maximum number of entries

        Returns:
            Formatted string of CPU profiling stats
        """
        if not result.cprofile_output_path:
            return ""

        profiler = CPUProfiler()
        return profiler.get_stats_string(result.cprofile_output_path, limit=limit)

    @property
    def memray_available(self) -> bool:
        """Check if memray is available."""
        return MEMRAY_AVAILABLE
