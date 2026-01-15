"""Data models for profiling results."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class ProfileResult:
    """Container for profiling results from memory and CPU profiling."""

    task_name: str
    start_time: datetime
    end_time: datetime
    duration_seconds: float

    # Memory metrics (in bytes)
    memory_peak_bytes: int = 0
    memory_start_bytes: int = 0
    memory_end_bytes: int = 0
    memory_allocated_bytes: int = 0

    # CPU metrics
    cpu_time_user: float = 0.0
    cpu_time_system: float = 0.0
    cpu_percent_avg: float = 0.0
    cpu_percent_peak: float = 0.0

    # Profiling artifacts
    memray_output_path: str | None = None
    cprofile_output_path: str | None = None

    # Additional metadata
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def memory_peak_mb(self) -> float:
        """Peak memory in megabytes."""
        return self.memory_peak_bytes / (1024 * 1024)

    @property
    def memory_allocated_mb(self) -> float:
        """Total allocated memory in megabytes."""
        return self.memory_allocated_bytes / (1024 * 1024)

    @property
    def cpu_time_total(self) -> float:
        """Total CPU time (user + system)."""
        return self.cpu_time_user + self.cpu_time_system

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for logging/serialization."""
        return {
            "task_name": self.task_name,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "duration_seconds": round(self.duration_seconds, 3),
            "memory_peak_mb": round(self.memory_peak_mb, 2),
            "memory_allocated_mb": round(self.memory_allocated_mb, 2),
            "cpu_time_user": round(self.cpu_time_user, 3),
            "cpu_time_system": round(self.cpu_time_system, 3),
            "cpu_time_total": round(self.cpu_time_total, 3),
            "cpu_percent_avg": round(self.cpu_percent_avg, 1),
            "cpu_percent_peak": round(self.cpu_percent_peak, 1),
            "memray_output_path": self.memray_output_path,
            "cprofile_output_path": self.cprofile_output_path,
            "metadata": self.metadata,
        }

    def summary(self) -> str:
        """Human-readable summary of profiling results."""
        lines = [
            f"=== Profile Results: {self.task_name} ===",
            f"Duration: {self.duration_seconds:.3f}s",
            "",
            "Memory:",
            f"  Peak: {self.memory_peak_mb:.2f} MB",
            f"  Allocated: {self.memory_allocated_mb:.2f} MB",
            "",
            "CPU:",
            f"  User time: {self.cpu_time_user:.3f}s",
            f"  System time: {self.cpu_time_system:.3f}s",
            f"  Avg usage: {self.cpu_percent_avg:.1f}%",
            f"  Peak usage: {self.cpu_percent_peak:.1f}%",
        ]
        if self.memray_output_path:
            lines.append(f"\nMemray output: {self.memray_output_path}")
        if self.cprofile_output_path:
            lines.append(f"cProfile output: {self.cprofile_output_path}")
        return "\n".join(lines)
