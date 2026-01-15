"""Example Dagster assets with integrated profiling.

These assets demonstrate how to use the ProfilingResource to profile
memory and CPU usage of different types of workloads.
"""

import hashlib
import math
import time
from typing import Any

import numpy as np
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetOut,
    MetadataValue,
    Output,
    asset,
    multi_asset,
)

from dagster_profiler.resources import ProfilingResource


@asset(
    description="Generate a large dataset to test memory profiling",
    metadata={"profile_type": "memory_heavy"},
)
def generate_large_dataset(
    context: AssetExecutionContext,
    profiler: ProfilingResource,
) -> Output[dict[str, Any]]:
    """Generate a large pandas DataFrame to exercise memory allocation.

    This asset creates a DataFrame with random data to test how well
    the profiler tracks memory allocations.
    """
    with profiler.profile("generate_large_dataset") as result:
        # Generate a moderately large dataset
        n_rows = 100_000
        n_cols = 50

        context.log.info(f"Generating DataFrame with {n_rows:,} rows and {n_cols} columns")

        # Create DataFrame with various data types
        data = {
            f"int_col_{i}": np.random.randint(0, 1000000, size=n_rows)
            for i in range(n_cols // 3)
        }
        data.update({
            f"float_col_{i}": np.random.randn(n_rows)
            for i in range(n_cols // 3)
        })
        data.update({
            f"str_col_{i}": [f"value_{j}" for j in np.random.randint(0, 1000, size=n_rows)]
            for i in range(n_cols // 3)
        })

        df = pd.DataFrame(data)
        shape = df.shape
        memory_usage = df.memory_usage(deep=True).sum()

    profiler.log_result(result)

    return Output(
        value={
            "shape": list(shape),  # Convert tuple to list for JSON serialization
            "memory_bytes": int(memory_usage),  # Convert numpy int to Python int
            "profile": result.to_dict(),
        },
        metadata={
            "rows": shape[0],
            "columns": shape[1],
            "dataframe_memory_mb": MetadataValue.float(float(memory_usage) / (1024 * 1024)),
            "peak_memory_mb": MetadataValue.float(result.memory_peak_mb),
            "duration_seconds": MetadataValue.float(result.duration_seconds),
        },
    )


@asset(
    description="Process data with high memory usage",
    metadata={"profile_type": "memory_heavy"},
)
def memory_heavy_processing(
    context: AssetExecutionContext,
    profiler: ProfilingResource,
    generate_large_dataset: dict[str, Any],
) -> Output[dict[str, Any]]:
    """Perform memory-intensive operations on data.

    This asset creates multiple copies of data structures to exercise
    memory allocation and tracking.
    """
    with profiler.profile("memory_heavy_processing") as result:
        # Create multiple large data structures
        arrays = []
        n_arrays = 5
        array_size = 1_000_000

        context.log.info(f"Creating {n_arrays} arrays of size {array_size:,}")

        for i in range(n_arrays):
            # Create array and do some operations that allocate memory
            arr = np.random.randn(array_size)
            # Operations that create intermediate arrays
            processed = np.sqrt(np.abs(arr)) * np.log1p(np.abs(arr))
            arrays.append(processed)

        # Concatenate all arrays (allocates more memory)
        combined = np.concatenate(arrays)
        stats = {
            "mean": float(np.mean(combined)),
            "std": float(np.std(combined)),
            "min": float(np.min(combined)),
            "max": float(np.max(combined)),
        }

        # Clean up to show memory being freed
        del arrays
        del combined

    profiler.log_result(result)

    return Output(
        value={
            "stats": stats,
            "profile": result.to_dict(),
        },
        metadata={
            "peak_memory_mb": MetadataValue.float(result.memory_peak_mb),
            "allocated_memory_mb": MetadataValue.float(result.memory_allocated_mb),
            "duration_seconds": MetadataValue.float(result.duration_seconds),
        },
    )


@asset(
    description="Perform CPU-intensive numerical computation",
    metadata={"profile_type": "cpu_heavy"},
)
def cpu_intensive_computation(
    context: AssetExecutionContext,
    profiler: ProfilingResource,
) -> Output[dict[str, Any]]:
    """Perform CPU-bound computations to test CPU profiling.

    This asset performs mathematical calculations that stress the CPU
    without using much memory.
    """
    with profiler.profile("cpu_intensive_computation") as result:
        # Prime number computation (CPU intensive)
        n_primes = 10000
        context.log.info(f"Computing first {n_primes:,} prime numbers")

        primes = []
        candidate = 2
        while len(primes) < n_primes:
            is_prime = True
            for p in primes:
                if p * p > candidate:
                    break
                if candidate % p == 0:
                    is_prime = False
                    break
            if is_prime:
                primes.append(candidate)
            candidate += 1

        # Matrix operations (uses optimized BLAS, but still CPU work)
        context.log.info("Performing matrix computations")
        n = 500
        a = np.random.randn(n, n)
        b = np.random.randn(n, n)

        # Multiple matrix multiplications
        for _ in range(10):
            c = np.dot(a, b)
            a = c / np.linalg.norm(c)

        # Hash computation (pure CPU)
        context.log.info("Computing hashes")
        hash_results = []
        for i in range(50000):
            h = hashlib.sha256(f"data_{i}".encode()).hexdigest()
            hash_results.append(h)

    profiler.log_result(result)

    return Output(
        value={
            "n_primes": len(primes),
            "largest_prime": primes[-1],
            "n_hashes": len(hash_results),
            "profile": result.to_dict(),
        },
        metadata={
            "cpu_time_user": MetadataValue.float(result.cpu_time_user),
            "cpu_time_system": MetadataValue.float(result.cpu_time_system),
            "cpu_percent_avg": MetadataValue.float(result.cpu_percent_avg),
            "cpu_percent_peak": MetadataValue.float(result.cpu_percent_peak),
            "duration_seconds": MetadataValue.float(result.duration_seconds),
        },
    )


@asset(
    description="Transform data with mixed CPU and memory usage",
    metadata={"profile_type": "mixed"},
)
def data_transformation(
    context: AssetExecutionContext,
    profiler: ProfilingResource,
    generate_large_dataset: dict[str, Any],
) -> Output[dict[str, Any]]:
    """Perform data transformations that use both CPU and memory.

    This asset demonstrates profiling of typical data processing workloads
    that involve both computation and data manipulation.
    """
    with profiler.profile("data_transformation") as result:
        # Recreate a dataset for transformation
        n_rows = 50_000

        context.log.info(f"Creating and transforming dataset with {n_rows:,} rows")

        # Create initial data
        df = pd.DataFrame({
            "id": range(n_rows),
            "value_a": np.random.randn(n_rows),
            "value_b": np.random.randn(n_rows),
            "category": np.random.choice(["A", "B", "C", "D"], size=n_rows),
            "timestamp": pd.date_range("2024-01-01", periods=n_rows, freq="s"),
        })

        # Apply various transformations
        df["value_sum"] = df["value_a"] + df["value_b"]
        df["value_product"] = df["value_a"] * df["value_b"]
        df["value_normalized"] = (df["value_a"] - df["value_a"].mean()) / df["value_a"].std()

        # String operations (memory intensive)
        df["category_upper"] = df["category"].str.upper()
        df["id_str"] = df["id"].astype(str).str.zfill(10)

        # Aggregations
        grouped = df.groupby("category").agg({
            "value_a": ["mean", "std", "min", "max"],
            "value_b": ["mean", "std", "min", "max"],
            "value_sum": "sum",
        })

        # Sort operations
        df_sorted = df.sort_values(["category", "value_sum"], ascending=[True, False])

        output_stats = {
            "final_shape": list(df_sorted.shape),  # Convert tuple to list
            "categories": int(grouped.shape[0]),  # Convert numpy int to Python int
            "memory_usage": int(df_sorted.memory_usage(deep=True).sum()),
        }

    profiler.log_result(result)

    return Output(
        value={
            "stats": output_stats,
            "profile": result.to_dict(),
        },
        metadata={
            "peak_memory_mb": MetadataValue.float(result.memory_peak_mb),
            "cpu_time_total": MetadataValue.float(result.cpu_time_total),
            "duration_seconds": MetadataValue.float(result.duration_seconds),
        },
    )


@asset(
    description="Aggregate data from multiple sources",
    metadata={"profile_type": "mixed"},
)
def data_aggregation(
    context: AssetExecutionContext,
    profiler: ProfilingResource,
    memory_heavy_processing: dict[str, Any],
    cpu_intensive_computation: dict[str, Any],
    data_transformation: dict[str, Any],
) -> Output[dict[str, Any]]:
    """Aggregate results from multiple upstream assets.

    This asset combines profiling data from multiple sources to create
    a summary of the entire pipeline's performance.
    """
    with profiler.profile("data_aggregation") as result:
        profiles = {
            "memory_heavy_processing": memory_heavy_processing.get("profile", {}),
            "cpu_intensive_computation": cpu_intensive_computation.get("profile", {}),
            "data_transformation": data_transformation.get("profile", {}),
        }

        # Calculate aggregate statistics
        total_duration = sum(p.get("duration_seconds", 0) for p in profiles.values())
        total_cpu_time = sum(p.get("cpu_time_total", 0) for p in profiles.values())
        max_memory = max(p.get("memory_peak_mb", 0) for p in profiles.values())

        # Simulate some aggregation work
        time.sleep(0.1)  # Minimal sleep to ensure profiling captures something

        aggregated = {
            "total_duration_seconds": total_duration,
            "total_cpu_time_seconds": total_cpu_time,
            "max_peak_memory_mb": max_memory,
            "task_count": len(profiles),
            "profiles": profiles,
        }

    profiler.log_result(result)

    return Output(
        value={
            "aggregated": aggregated,
            "profile": result.to_dict(),
        },
        metadata={
            "total_pipeline_duration": MetadataValue.float(total_duration),
            "max_memory_mb": MetadataValue.float(max_memory),
            "duration_seconds": MetadataValue.float(result.duration_seconds),
        },
    )


@asset(
    description="Generate a summary report of all profiling results",
    metadata={"profile_type": "summary"},
)
def profile_results_summary(
    context: AssetExecutionContext,
    profiler: ProfilingResource,
    data_aggregation: dict[str, Any],
) -> Output[dict[str, Any]]:
    """Create a final summary of all profiling results.

    This asset compiles all profiling data into a comprehensive report.
    """
    with profiler.profile("profile_results_summary") as result:
        aggregated = data_aggregation.get("aggregated", {})
        profiles = aggregated.get("profiles", {})

        # Build summary report
        report_lines = [
            "=" * 60,
            "DAGSTER PERFORMANCE PROFILING SUMMARY",
            "=" * 60,
            "",
            f"Total Tasks Profiled: {aggregated.get('task_count', 0)}",
            f"Total Duration: {aggregated.get('total_duration_seconds', 0):.3f}s",
            f"Total CPU Time: {aggregated.get('total_cpu_time_seconds', 0):.3f}s",
            f"Peak Memory (max across tasks): {aggregated.get('max_peak_memory_mb', 0):.2f} MB",
            "",
            "-" * 60,
            "INDIVIDUAL TASK RESULTS",
            "-" * 60,
        ]

        for task_name, profile in profiles.items():
            report_lines.extend([
                "",
                f"Task: {task_name}",
                f"  Duration: {profile.get('duration_seconds', 0):.3f}s",
                f"  Memory Peak: {profile.get('memory_peak_mb', 0):.2f} MB",
                f"  CPU User Time: {profile.get('cpu_time_user', 0):.3f}s",
                f"  CPU System Time: {profile.get('cpu_time_system', 0):.3f}s",
            ])

        report_lines.extend(["", "=" * 60])
        report = "\n".join(report_lines)

        context.log.info(report)

    profiler.log_result(result)

    return Output(
        value={
            "report": report,
            "aggregated_stats": aggregated,
            "profile": result.to_dict(),
        },
        metadata={
            "report_length": len(report),
            "duration_seconds": MetadataValue.float(result.duration_seconds),
        },
    )


# Multi-asset example showing parallel profiling
@multi_asset(
    outs={
        "parallel_task_a": AssetOut(description="First parallel profiled task"),
        "parallel_task_b": AssetOut(description="Second parallel profiled task"),
    },
    description="Demonstrate profiling of parallel tasks",
)
def parallel_profiled_tasks(
    context: AssetExecutionContext,
    profiler: ProfilingResource,
) -> tuple[Output[dict[str, Any]], Output[dict[str, Any]]]:
    """Execute and profile multiple independent tasks.

    This multi-asset demonstrates how to profile tasks that could
    potentially run in parallel (though in this case they run sequentially
    within the same op).
    """
    # Profile task A
    with profiler.profile("parallel_task_a") as result_a:
        # Simulate memory-bound work
        data_a = [np.random.randn(10000) for _ in range(100)]
        sum_a = sum(np.sum(arr) for arr in data_a)

    profiler.log_result(result_a)

    # Profile task B
    with profiler.profile("parallel_task_b") as result_b:
        # Simulate CPU-bound work
        result = 0
        for i in range(100000):
            result += math.sin(i) * math.cos(i)

    profiler.log_result(result_b)

    return (
        Output(
            value={"sum": float(sum_a), "profile": result_a.to_dict()},
            metadata={
                "peak_memory_mb": MetadataValue.float(result_a.memory_peak_mb),
                "duration_seconds": MetadataValue.float(result_a.duration_seconds),
            },
        ),
        Output(
            value={"result": result, "profile": result_b.to_dict()},
            metadata={
                "cpu_time_total": MetadataValue.float(result_b.cpu_time_total),
                "duration_seconds": MetadataValue.float(result_b.duration_seconds),
            },
        ),
    )
