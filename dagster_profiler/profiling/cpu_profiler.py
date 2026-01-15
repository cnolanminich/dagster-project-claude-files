"""CPU profiling using cProfile and psutil."""

import cProfile
import os
import pstats
import tempfile
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from functools import wraps
from io import StringIO
from pathlib import Path
from typing import Any, Callable, Generator

import psutil

from dagster_profiler.profiling.models import ProfileResult


class CPUMonitor:
    """Background thread for monitoring CPU usage over time."""

    def __init__(self, interval: float = 0.1):
        """Initialize CPU monitor.

        Args:
            interval: Sampling interval in seconds
        """
        self.interval = interval
        self.samples: list[float] = []
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._process = psutil.Process(os.getpid())

    def start(self) -> None:
        """Start monitoring CPU usage in background."""
        self._stop_event.clear()
        self.samples = []
        self._thread = threading.Thread(target=self._monitor, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop monitoring and wait for thread to finish."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=1.0)

    def _monitor(self) -> None:
        """Background monitoring loop."""
        while not self._stop_event.is_set():
            try:
                cpu_percent = self._process.cpu_percent(interval=None)
                self.samples.append(cpu_percent)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                break
            time.sleep(self.interval)

    @property
    def average(self) -> float:
        """Average CPU usage across all samples."""
        return sum(self.samples) / len(self.samples) if self.samples else 0.0

    @property
    def peak(self) -> float:
        """Peak CPU usage observed."""
        return max(self.samples) if self.samples else 0.0


class CPUProfiler:
    """CPU profiler using cProfile for detailed function-level profiling."""

    def __init__(
        self,
        output_dir: str | None = None,
        monitor_interval: float = 0.1,
        enable_cprofile: bool = True,
    ):
        """Initialize CPU profiler.

        Args:
            output_dir: Directory to store profiling output files
            monitor_interval: CPU monitoring sample interval in seconds
            enable_cprofile: Whether to enable cProfile for detailed profiling
        """
        self.output_dir = output_dir or tempfile.mkdtemp(prefix="dagster_profile_")
        self.monitor_interval = monitor_interval
        self.enable_cprofile = enable_cprofile
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

    @contextmanager
    def profile(self, task_name: str) -> Generator[ProfileResult, None, None]:
        """Context manager for CPU profiling.

        Args:
            task_name: Name of the task being profiled

        Yields:
            ProfileResult that will be populated with CPU metrics after execution
        """
        result = ProfileResult(
            task_name=task_name,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_seconds=0.0,
        )

        process = psutil.Process(os.getpid())
        cpu_times_start = process.cpu_times()

        # Start CPU monitoring
        monitor = CPUMonitor(interval=self.monitor_interval)
        monitor.start()

        # Setup cProfile
        profiler = cProfile.Profile() if self.enable_cprofile else None
        if profiler:
            profiler.enable()

        try:
            yield result
        finally:
            # Stop cProfile
            if profiler:
                profiler.disable()

            # Stop CPU monitor
            monitor.stop()

            result.end_time = datetime.now()
            result.duration_seconds = (result.end_time - result.start_time).total_seconds()

            # Get CPU times
            cpu_times_end = process.cpu_times()
            result.cpu_time_user = cpu_times_end.user - cpu_times_start.user
            result.cpu_time_system = cpu_times_end.system - cpu_times_start.system

            # Get CPU usage stats
            result.cpu_percent_avg = monitor.average
            result.cpu_percent_peak = monitor.peak

            # Save cProfile output
            if profiler:
                output_path = os.path.join(
                    self.output_dir,
                    f"{task_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.prof",
                )
                profiler.dump_stats(output_path)
                result.cprofile_output_path = output_path

    def get_stats_string(
        self,
        prof_file: str,
        sort_by: str = "cumulative",
        limit: int = 20,
    ) -> str:
        """Get a string representation of cProfile stats.

        Args:
            prof_file: Path to .prof file
            sort_by: Sort key ('cumulative', 'time', 'calls')
            limit: Maximum number of entries to show

        Returns:
            Formatted string of profiling stats
        """
        if not os.path.exists(prof_file):
            return ""

        stream = StringIO()
        stats = pstats.Stats(prof_file, stream=stream)
        stats.strip_dirs()
        stats.sort_stats(sort_by)
        stats.print_stats(limit)
        return stream.getvalue()

    def get_callers(self, prof_file: str, function_name: str) -> str:
        """Get callers of a specific function.

        Args:
            prof_file: Path to .prof file
            function_name: Name of function to analyze

        Returns:
            Formatted string of callers
        """
        if not os.path.exists(prof_file):
            return ""

        stream = StringIO()
        stats = pstats.Stats(prof_file, stream=stream)
        stats.strip_dirs()
        stats.print_callers(function_name)
        return stream.getvalue()


def profile_cpu(
    task_name: str | None = None,
    output_dir: str | None = None,
    enable_cprofile: bool = True,
) -> Callable:
    """Decorator for CPU profiling a function.

    Args:
        task_name: Name for the profiled task (defaults to function name)
        output_dir: Directory for profiling output
        enable_cprofile: Whether to enable detailed cProfile

    Returns:
        Decorated function that profiles CPU usage
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> tuple[Any, ProfileResult]:
            name = task_name or func.__name__
            profiler = CPUProfiler(output_dir=output_dir, enable_cprofile=enable_cprofile)

            with profiler.profile(name) as result:
                return_value = func(*args, **kwargs)

            return return_value, result

        return wrapper

    return decorator
