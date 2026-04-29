"""
run_tests.py
Automation suite for PAMIP.
Discovers and runs all test modules under tests/, prints a per-module
summary, and exits with code 1 if any failures occurred.

Usage (run from the pamip/ project root):
    python run_tests.py
"""

import sys
import importlib
import time


# ----------------------------------------
# Test modules
# Each entry is the import path as it would appear from the project root.
# ----------------------------------------

TEST_MODULES = [
    "tests.test_models",
    "tests.test_watcher",
    "tests.test_executor",
    "tests.test_repositories",
    "tests.test_loop",
    "tests.test_steps",
    "tests.test_engine",
    "tests.test_commands",
    "tests.test_config",
]


# ----------------------------------------
# Formatting
# ----------------------------------------

_DIVIDER = "-" * 52
_HEADER  = "=" * 52


def _print_header():
    print(f"\n{_HEADER}")
    print("  PAMIP Test Suite")
    print(_HEADER)


def _print_module_banner(name: str):
    label = name.split(".")[-1]          # e.g. "test_models"
    print(f"\n{_DIVIDER}")
    print(f"  {label}")
    print(_DIVIDER)


def _print_summary(results: list[tuple[str, int, float]]):
    """
    _print_summary
    Prints the final pass/fail table across all modules.

    Args:
        results (list of (module_name, failure_count, elapsed_seconds))
    """
    total_failures = sum(f for _, f, _ in results)
    total_time     = sum(t for _, _, t in results)

    print(f"\n{_HEADER}")
    print("  Results")
    print(_HEADER)

    for name, failures, elapsed in results:
        label  = name.split(".")[-1]
        status = "PASS" if failures == 0 else f"FAIL ({failures} failed)"
        print(f"  {label:<30} {status:<20} {elapsed:.2f}s")

    print(_DIVIDER)
    overall = "ALL PASSED" if total_failures == 0 else f"{total_failures} FAILURE(S)"
    print(f"  {overall:<30} {'':20} {total_time:.2f}s total")
    print(_HEADER)
    print()


# ----------------------------------------
# Runner
# ----------------------------------------

def run_all() -> int:
    """
    run_all
    Imports and runs each test module in TEST_MODULES.
    Returns the total number of failures across all modules.
    """
    _print_header()

    results = []

    for module_path in TEST_MODULES:
        _print_module_banner(module_path)

        try:
            module = importlib.import_module(module_path)
        except ImportError as e:
            print(f"  ERROR: could not import {module_path}: {e}")
            results.append((module_path, 1, 0.0))
            continue

        if not hasattr(module, "run_tests"):
            print(f"  ERROR: {module_path} has no run_tests() function.")
            results.append((module_path, 1, 0.0))
            continue

        start    = time.monotonic()
        failures = module.run_tests() or 0
        elapsed  = time.monotonic() - start

        results.append((module_path, failures, elapsed))

    _print_summary(results)
    return sum(f for _, f, _ in results)


if __name__ == "__main__":
    sys.exit(1 if run_all() else 0)
