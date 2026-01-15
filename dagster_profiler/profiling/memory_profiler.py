"""Memory profiling using memray and tracemalloc."""

import os
import tempfile
import tracemalloc
from contextlib import contextmanager
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Generator

import psutil

from dagster_profiler.profiling.models import ProfileResult

# Try to import memray - it may not be available on all platforms
try:
    import memray

    MEMRAY_AVAILABLE = True
except ImportError:
    MEMRAY_AVAILABLE = False


class MemoryProfiler:
    """Memory profiler that uses memray for detailed profiling and tracemalloc as fallback."""

    def __init__(
        self,
        output_dir: str | None = None,
        use_memray: bool = True,
        track_native: bool = False,
    ):
        """Initialize memory profiler.

        Args:
            output_dir: Directory to store profiling output files
            use_memray: Whether to use memray (if available) for detailed profiling
            track_native: Whether to track native (C/C++) allocations (memray only)
        """
        self.output_dir = output_dir or tempfile.mkdtemp(prefix="dagster_profile_")
        self.use_memray = use_memray and MEMRAY_AVAILABLE
        self.track_native = track_native
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

    @contextmanager
    def profile(self, task_name: str) -> Generator[ProfileResult, None, None]:
        """Context manager for memory profiling.

        Args:
            task_name: Name of the task being profiled

        Yields:
            ProfileResult that will be populated with memory metrics after execution
        """
        result = ProfileResult(
            task_name=task_name,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_seconds=0.0,
        )

        process = psutil.Process(os.getpid())
        mem_info_start = process.memory_info()
        result.memory_start_bytes = mem_info_start.rss

        # Start tracemalloc for basic tracking
        tracemalloc.start()

        memray_path = None
        memray_tracker = None

        if self.use_memray:
            memray_path = os.path.join(
                self.output_dir, f"{task_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.bin"
            )
            result.memray_output_path = memray_path
            memray_tracker = memray.Tracker(
                memray_path,
                native_traces=self.track_native,
            )
            memray_tracker.__enter__()

        try:
            yield result
        finally:
            result.end_time = datetime.now()
            result.duration_seconds = (result.end_time - result.start_time).total_seconds()

            # Get tracemalloc stats
            current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            result.memory_allocated_bytes = current
            result.memory_peak_bytes = peak

            # Get final memory usage
            mem_info_end = process.memory_info()
            result.memory_end_bytes = mem_info_end.rss

            if memray_tracker:
                memray_tracker.__exit__(None, None, None)

    def generate_report(self, bin_file: str, output_format: str = "html") -> str | None:
        """Generate a report from a memray binary file.

        Args:
            bin_file: Path to memray .bin file
            output_format: Output format ('html', 'flamegraph', 'table', 'tree', 'stats')

        Returns:
            Path to generated report file, or None if generation failed
        """
        if not MEMRAY_AVAILABLE:
            return None

        if not os.path.exists(bin_file):
            return None

        output_path = bin_file.replace(".bin", f".{output_format}")

        try:
            if output_format == "html":
                from memray.commands.flamegraph import FlamegraphCommand

                cmd = FlamegraphCommand()
                cmd.main([bin_file, "-o", output_path, "--force"])
            elif output_format == "stats":
                from memray.commands.stats import StatsCommand

                output_path = bin_file.replace(".bin", "_stats.txt")
                cmd = StatsCommand()
                # Stats command writes to stdout, we'd need to capture it
                cmd.main([bin_file])
                return None
            elif output_format == "tree":
                from memray.commands.tree import TreeCommand

                cmd = TreeCommand()
                cmd.main([bin_file])
                return None
            else:
                return None

            return output_path if os.path.exists(output_path) else None
        except Exception:
            return None


def profile_memory(
    task_name: str | None = None,
    output_dir: str | None = None,
    use_memray: bool = True,
) -> Callable:
    """Decorator for memory profiling a function.

    Args:
        task_name: Name for the profiled task (defaults to function name)
        output_dir: Directory for profiling output
        use_memray: Whether to use memray for detailed profiling

    Returns:
        Decorated function that profiles memory usage
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> tuple[Any, ProfileResult]:
            name = task_name or func.__name__
            profiler = MemoryProfiler(output_dir=output_dir, use_memray=use_memray)

            with profiler.profile(name) as result:
                return_value = func(*args, **kwargs)

            return return_value, result

        return wrapper

    return decorator
