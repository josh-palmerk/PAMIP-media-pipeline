"""
test_engine.py
Tests for PipelineEngine in pipeline/engine.py.
Verifies the field mapping between executor.ExecutionResult and the
result dict that JobManager.process_job() expects, plus error handling
for unknown step names.

Subprocess execution is patched out — these tests focus on the bridge
logic, not on running real commands.

Run from the project root:
    python -m tests.test_engine
"""

import sys
from dataclasses import dataclass


# Lightweight stand-ins for Job and Step so we don't depend on the real
# dataclasses for these wiring tests.

@dataclass
class _FakeJob:
    id:        int = 1
    file_path: str = "/media/clip.mp4"


@dataclass
class _FakeStep:
    id:         int = 10
    step_name:  str = "transcode"


@dataclass
class _FakeResult:
    """Mimics tools.executor.ExecutionResult for the fields the engine reads."""
    success:   bool
    exit_code: int
    stdout:    str
    stderr:    str
    timed_out: bool = False


def run_tests():
    from pipeline import engine as engine_module
    from pipeline.engine import PipelineEngine
    from pipeline.steps import register_step
    from config import StepConfig

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

    # Save the real run_process so we can restore it after each test
    original_run_process = engine_module.run_process

    def patch_run_process(fn):
        """Monkey-patch run_process inside the engine module."""
        engine_module.run_process = fn

    def restore_run_process():
        engine_module.run_process = original_run_process

    # Register a benign handler we can wire to a synthetic step name.
    # The engine looks up by step_name, so this avoids interfering with
    # the real "transcode" handler's file-type routing.
    @register_step("__engine_test_step__")
    def _test_handler(file_path, output_dir, job, options):
        # Stash the call args on the function itself so tests can inspect them
        _test_handler.last_call = {
            "file_path": file_path,
            "output_dir": output_dir,
            "job": job,
            "options": options,
        }
        return ["pretend", "command"]

    # ----------------------------------------
    # Field mapping: success path
    # ----------------------------------------
    print("\n--- field mapping ---")

    captured = {}
    def fake_success(command, timeout_seconds):
        captured["command"] = command
        captured["timeout"] = timeout_seconds
        return _FakeResult(
            success=True,
            exit_code=0,
            stdout="hello",
            stderr="",
        )

    patch_run_process(fake_success)
    try:
        engine = PipelineEngine(
            step_configs=[StepConfig("__engine_test_step__", 1, 42, {"foo": "bar"})],
            output_dir="/out",
        )
        result = engine.execute_step(_FakeStep(step_name="__engine_test_step__"),
                                     _FakeJob())

        check("result is a dict",                isinstance(result, dict))
        check("success forwarded",               result["success"] is True)
        check("exit_code forwarded",             result["exit_code"] == 0)
        check("stdout forwarded",                result["stdout"] == "hello")
        check("stderr forwarded",                result["stderr"] == "")
        # No timed_out leakage — engine intentionally drops it; manager doesn't use it
        check("timed_out NOT in returned dict",  "timed_out" not in result)
    finally:
        restore_run_process()

    # ----------------------------------------
    # Field mapping: failure path with stderr
    # ----------------------------------------
    def fake_failure(command, timeout_seconds):
        return _FakeResult(
            success=False,
            exit_code=2,
            stdout="partial",
            stderr="boom",
        )

    patch_run_process(fake_failure)
    try:
        engine = PipelineEngine(
            step_configs=[StepConfig("__engine_test_step__", 1, 60, {})],
            output_dir="/out",
        )
        result = engine.execute_step(_FakeStep(step_name="__engine_test_step__"),
                                     _FakeJob())

        check("failure: success is False",       result["success"] is False)
        check("failure: exit_code forwarded",    result["exit_code"] == 2)
        check("failure: stderr forwarded",       result["stderr"] == "boom")
        check("failure: stdout forwarded",       result["stdout"] == "partial")
    finally:
        restore_run_process()

    # ----------------------------------------
    # Handler call args — file_path/output_dir/options reach the handler
    # ----------------------------------------
    print("\n--- handler invocation ---")

    patch_run_process(fake_success)
    try:
        engine = PipelineEngine(
            step_configs=[StepConfig("__engine_test_step__", 1, 99, {"key": "value"})],
            output_dir="/configured/out",
        )
        engine.execute_step(_FakeStep(step_name="__engine_test_step__"),
                            _FakeJob(file_path="/media/in.mp4"))

        last = _test_handler.last_call
        check("handler receives file_path",      last["file_path"] == "/media/in.mp4")
        check("handler receives output_dir",     last["output_dir"] == "/configured/out")
        check("handler receives options",        last["options"] == {"key": "value"})
        check("handler receives job",            isinstance(last["job"], _FakeJob))

        # And the command/timeout get passed through to run_process unchanged
        check("run_process receives handler command",
              captured["command"] == ["pretend", "command"])
        check("run_process receives configured timeout",
              captured["timeout"] == 99)
    finally:
        restore_run_process()

    # ----------------------------------------
    # Unknown step name — fails gracefully without invoking executor
    # ----------------------------------------
    print("\n--- unknown step ---")

    executor_called = {"called": False}
    def must_not_be_called(command, timeout_seconds):
        executor_called["called"] = True
        return _FakeResult(success=True, exit_code=0, stdout="", stderr="")

    patch_run_process(must_not_be_called)
    try:
        engine = PipelineEngine(
            step_configs=[],   # nothing registered for "definitely_not_a_step"
            output_dir="/out",
        )
        result = engine.execute_step(_FakeStep(step_name="definitely_not_a_step"),
                                     _FakeJob())

        check("unknown step: success is False",  result["success"] is False)
        check("unknown step: exit_code -1",      result["exit_code"] == -1)
        check("unknown step: stderr mentions step name",
              "definitely_not_a_step" in result["stderr"])
        check("unknown step: executor NOT called",
              not executor_called["called"])
    finally:
        restore_run_process()

    # ----------------------------------------
    # Default timeout — when step_name has no entry in step_configs
    # ----------------------------------------
    print("\n--- default timeout ---")

    captured_timeout = {}
    def capture_timeout(command, timeout_seconds):
        captured_timeout["t"] = timeout_seconds
        return _FakeResult(success=True, exit_code=0, stdout="", stderr="")

    patch_run_process(capture_timeout)
    try:
        # Engine constructed with NO config for our test step
        engine = PipelineEngine(step_configs=[], output_dir="/out")
        engine.execute_step(_FakeStep(step_name="__engine_test_step__"),
                            _FakeJob())
        # engine.py uses 60s as fallback when step_name not in _timeouts
        check("default timeout used when step not in config",
              captured_timeout["t"] == 60)
    finally:
        restore_run_process()

    print(f"\nResults: {passed} passed, {failed} failed\n")
    return failed


if __name__ == "__main__":
    failures = run_tests()
    sys.exit(1 if failures else 0)
