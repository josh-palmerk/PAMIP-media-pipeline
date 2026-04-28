"""
test_executor.py
Tests for run_process() in tools/executor.py.
Uses simple shell commands to simulate success, failure, and timeout
without requiring any external media tools.

Run from the project root:
    python test_executor.py
"""

import sys


def run_tests():
    from tools.executor import run_process, ExecutionResult

    passed = 0
    failed = 0

    def check(name, condition):
        nonlocal passed, failed
        if condition:
            print(f"  PASS  {name}")
            passed += 1
        else:
            print(f"  FAIL  {name}")
            failed += 1

    # ----------------------------------------
    # Success Case
    # ----------------------------------------
    print("\n--- Success ---")

    result = run_process(
        ["python", "-c", "print('hello stdout')"],
        timeout_seconds=10
    )
    check("returns ExecutionResult",        isinstance(result, ExecutionResult))
    check("success is True",               result.success)
    check("exit_code is 0",                result.exit_code == 0)
    check("stdout captured",               "hello stdout" in result.stdout)
    check("timed_out is False",            not result.timed_out)

    # ----------------------------------------
    # Failure Case — non-zero exit code
    # ----------------------------------------
    print("\n--- Failure ---")

    result = run_process(
        ["python", "-c", "import sys; print('err', file=sys.stderr); sys.exit(1)"],
        timeout_seconds=10
    )
    check("success is False",              not result.success)
    check("exit_code is 1",               result.exit_code == 1)
    check("stderr captured",              "err" in result.stderr)
    check("timed_out is False",           not result.timed_out)

    # ----------------------------------------
    # Stdout and stderr both captured
    # ----------------------------------------
    print("\n--- Output Capture ---")

    result = run_process(
        ["python", "-c",
         "import sys; print('out line'); print('err line', file=sys.stderr)"],
        timeout_seconds=10
    )
    check("stdout captured",              "out line" in result.stdout)
    check("stderr captured",              "err line" in result.stderr)

    # ----------------------------------------
    # Timeout Case — FR-15
    # ----------------------------------------
    print("\n--- Timeout ---")

    result = run_process(
        ["python", "-c", "import time; time.sleep(30)"],
        timeout_seconds=2
    )
    check("success is False on timeout",  not result.success)
    check("exit_code is -1 on timeout",   result.exit_code == -1)
    check("timed_out is True",            result.timed_out)

    # ----------------------------------------
    # Multiline output preserved
    # ----------------------------------------
    print("\n--- Multiline Output ---")

    result = run_process(
        ["python", "-c", "for i in range(5): print(f'line {i}')"],
        timeout_seconds=10
    )
    check("all lines captured",           all(f"line {i}" in result.stdout for i in range(5)))

    print(f"\nResults: {passed} passed, {failed} failed\n")
    return failed


if __name__ == "__main__":
    failures = run_tests()
    sys.exit(1 if failures else 0)
