#!/usr/bin/env python
"""Test script to compare log output between Component and Pythonic approaches.

This script helps diagnose differences in how warnings and errors are surfaced
between the SlingReplicationCollectionComponent and the Pythonic @sling_assets approach.

Key things to look for:
1. "Downloading sling binary" message (from sling/bin.py print statement)
2. "Failed to download sling binary" UserWarning (from sling/bin.py warnings.warn)
3. Any RuntimeError about sling binary

Usage:
    python test_log_comparison.py
"""
import sys
import warnings
from io import StringIO
from contextlib import redirect_stdout, redirect_stderr

# Capture all warnings
captured_warnings = []
def warning_handler(message, category, filename, lineno, file=None, line=None):
    captured_warnings.append({
        'message': str(message),
        'category': category.__name__,
        'filename': filename,
        'lineno': lineno
    })
    # Also print to stderr so it's visible
    print(f"WARNING CAPTURED: [{category.__name__}] {message}", file=sys.stderr)

# Install the warning handler
old_showwarning = warnings.showwarning
warnings.showwarning = warning_handler


def test_import_sling_direct():
    """Test importing sling directly (outside any warning suppression)."""
    print("\n" + "="*60)
    print("TEST 1: Direct sling import (no suppression)")
    print("="*60)

    captured_warnings.clear()
    stdout_capture = StringIO()
    stderr_capture = StringIO()

    try:
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            import sling
            print(f"SLING_BIN = {sling.SLING_BIN}")
    except Exception as e:
        print(f"Exception: {type(e).__name__}: {e}")

    print(f"STDOUT:\n{stdout_capture.getvalue()}")
    print(f"STDERR:\n{stderr_capture.getvalue()}")
    print(f"Captured warnings: {len(captured_warnings)}")
    for w in captured_warnings:
        print(f"  - {w}")


def test_import_with_dagster_suppression():
    """Test importing with Dagster's warning suppression active."""
    print("\n" + "="*60)
    print("TEST 2: Import with @suppress_dagster_warnings")
    print("="*60)

    # Clear the sling module from cache so it reimports
    to_remove = [k for k in sys.modules.keys() if 'sling' in k.lower()]
    for k in to_remove:
        del sys.modules[k]

    captured_warnings.clear()
    stdout_capture = StringIO()
    stderr_capture = StringIO()

    try:
        from dagster._utils.warnings import suppress_dagster_warnings

        @suppress_dagster_warnings
        def import_sling():
            import sling
            return sling.SLING_BIN

        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            result = import_sling()
            print(f"SLING_BIN = {result}")
    except Exception as e:
        print(f"Exception: {type(e).__name__}: {e}")

    print(f"STDOUT:\n{stdout_capture.getvalue()}")
    print(f"STDERR:\n{stderr_capture.getvalue()}")
    print(f"Captured warnings: {len(captured_warnings)}")
    for w in captured_warnings:
        print(f"  - {w}")


def test_component_loading():
    """Test loading via load_from_defs_folder (Component approach)."""
    print("\n" + "="*60)
    print("TEST 3: Component loading via load_from_defs_folder")
    print("="*60)

    # Clear the sling module from cache
    to_remove = [k for k in sys.modules.keys() if 'sling' in k.lower() or 'dagster_sling' in k.lower()]
    for k in to_remove:
        del sys.modules[k]

    captured_warnings.clear()
    stdout_capture = StringIO()
    stderr_capture = StringIO()

    try:
        from pathlib import Path
        from dagster import load_from_defs_folder

        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            defs = load_from_defs_folder(path_within_project=Path(__file__).parent / "src" / "sling_logs_test")
            print(f"Loaded defs: {defs}")
            asset_keys = [str(spec.key) for spec in defs.get_asset_graph().asset_specs]
            print(f"Asset keys: {asset_keys}")
    except Exception as e:
        print(f"Exception: {type(e).__name__}: {e}")

    print(f"STDOUT:\n{stdout_capture.getvalue()}")
    print(f"STDERR:\n{stderr_capture.getvalue()}")
    print(f"Captured warnings: {len(captured_warnings)}")
    for w in captured_warnings:
        print(f"  - {w}")


def test_pythonic_import_direct():
    """Test direct import of pythonic_assets module."""
    print("\n" + "="*60)
    print("TEST 4: Direct import of pythonic_assets (Pythonic approach)")
    print("="*60)

    # Clear the sling module from cache
    to_remove = [k for k in sys.modules.keys() if 'sling' in k.lower() or 'dagster_sling' in k.lower()]
    for k in to_remove:
        del sys.modules[k]

    captured_warnings.clear()
    stdout_capture = StringIO()
    stderr_capture = StringIO()

    try:
        import sys
        from pathlib import Path
        # Add the src directory to path
        src_path = Path(__file__).parent / "src"
        if str(src_path) not in sys.path:
            sys.path.insert(0, str(src_path))

        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            from sling_logs_test.defs import pythonic_assets
            print(f"Loaded defs: {pythonic_assets.defs}")
    except Exception as e:
        print(f"Exception: {type(e).__name__}: {e}")

    print(f"STDOUT:\n{stdout_capture.getvalue()}")
    print(f"STDERR:\n{stderr_capture.getvalue()}")
    print(f"Captured warnings: {len(captured_warnings)}")
    for w in captured_warnings:
        print(f"  - {w}")


if __name__ == "__main__":
    print("="*60)
    print("SLING LOG COMPARISON TEST")
    print("="*60)
    print("\nThis test compares how warnings are surfaced in different loading scenarios.")
    print("Look for 'Downloading sling binary' and 'Failed to download' messages.\n")

    # Run tests in order
    test_import_sling_direct()
    test_import_with_dagster_suppression()

    # These tests require the full project structure
    try:
        test_pythonic_import_direct()
        test_component_loading()
    except Exception as e:
        print(f"Skipping project-level tests: {e}")

    # Restore original warning handler
    warnings.showwarning = old_showwarning

    print("\n" + "="*60)
    print("TESTS COMPLETE")
    print("="*60)
