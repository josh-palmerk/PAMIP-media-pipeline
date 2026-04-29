"""
test_config.py
Tests for config validation and loading in config.py.
Validates the _validate() rules and load_config() success/failure paths
against synthetic JSON files written to a temporary directory.

Run from the project root:
    python -m tests.test_config
"""

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch


def run_tests():
    import config as config_module
    from config import load_config, _validate, Config, StepConfig

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
    # _validate
    # ----------------------------------------
    print("\n--- _validate ---")

    valid = {
        "watch_directory":        "./in",
        "output_directory":       "./out",
        "allowed_extensions":     [".mp4"],
        "poll_interval_seconds":  5,
        "max_concurrent_jobs":    1,
        "pipeline": [
            {"step_name": "transcode", "max_attempts": 3, "timeout_seconds": 60},
        ],
    }

    try:
        _validate(valid)
        check("_validate: valid config passes", True)
    except ValueError:
        check("_validate: valid config passes", False)

    # Missing top-level key
    for missing in ["watch_directory", "pipeline", "max_concurrent_jobs"]:
        bad = dict(valid)
        del bad[missing]
        try:
            _validate(bad)
            check(f"_validate: missing '{missing}' raises", False)
        except ValueError as e:
            check(f"_validate: missing '{missing}' raises",
                  missing in str(e))

    # Empty pipeline list
    bad = dict(valid)
    bad["pipeline"] = []
    try:
        _validate(bad)
        check("_validate: empty pipeline raises", False)
    except ValueError:
        check("_validate: empty pipeline raises", True)

    # Pipeline is not a list
    bad = dict(valid)
    bad["pipeline"] = "transcode"
    try:
        _validate(bad)
        check("_validate: non-list pipeline raises", False)
    except ValueError:
        check("_validate: non-list pipeline raises", True)

    # Pipeline step missing a required key
    for missing in ["step_name", "max_attempts", "timeout_seconds"]:
        bad = dict(valid)
        bad["pipeline"] = [{"step_name": "transcode", "max_attempts": 3, "timeout_seconds": 60}]
        del bad["pipeline"][0][missing]
        try:
            _validate(bad)
            check(f"_validate: step missing '{missing}' raises", False)
        except ValueError as e:
            check(f"_validate: step missing '{missing}' raises",
                  missing in str(e))

    # ----------------------------------------
    # load_config
    # ----------------------------------------
    print("\n--- load_config ---")

    def with_config_path(tmp_path: Path):
        """
        Patch the module-level CONFIG_PATH so load_config reads from our
        temp directory instead of the project's real config/config.json.
        """
        return patch.object(config_module, "CONFIG_PATH", tmp_path)

    # Successful load
    with tempfile.TemporaryDirectory() as tmp:
        cfg_path = Path(tmp) / "config.json"
        cfg_path.write_text(json.dumps(valid))

        with with_config_path(cfg_path):
            cfg = load_config()

        check("load_config: returns Config",          isinstance(cfg, Config))
        check("load_config: watch_directory",         cfg.watch_directory == "./in")
        check("load_config: poll_interval",           cfg.poll_interval_seconds == 5)
        check("load_config: pipeline length",         len(cfg.pipeline) == 1)
        check("load_config: pipeline element type",   isinstance(cfg.pipeline[0], StepConfig))
        check("load_config: step_name parsed",        cfg.pipeline[0].step_name == "transcode")
        check("load_config: timeout_seconds parsed",  cfg.pipeline[0].timeout_seconds == 60)
        # options defaults to {} when omitted
        check("load_config: options defaults to empty dict",
              cfg.pipeline[0].options == {})

    # Step options propagated
    with tempfile.TemporaryDirectory() as tmp:
        cfg_path = Path(tmp) / "config.json"
        valid_with_options = json.loads(json.dumps(valid))  # deep copy
        valid_with_options["pipeline"][0]["options"] = {"compress_threshold_mb": 5}
        cfg_path.write_text(json.dumps(valid_with_options))

        with with_config_path(cfg_path):
            cfg = load_config()

        check("load_config: options propagated",
              cfg.pipeline[0].options == {"compress_threshold_mb": 5})

    # Default config written when none exists
    with tempfile.TemporaryDirectory() as tmp:
        cfg_path = Path(tmp) / "subdir" / "config.json"
        # Sanity: file does not exist yet
        check("load_config: precondition — file missing", not cfg_path.exists())

        with with_config_path(cfg_path):
            cfg = load_config()

        check("load_config: creates default config file when missing",
              cfg_path.exists())
        check("load_config: default config loads as Config",
              isinstance(cfg, Config))
        check("load_config: default has non-empty pipeline",
              len(cfg.pipeline) > 0)

    # Malformed config raises
    with tempfile.TemporaryDirectory() as tmp:
        cfg_path = Path(tmp) / "config.json"
        bad = dict(valid)
        del bad["pipeline"]
        cfg_path.write_text(json.dumps(bad))

        with with_config_path(cfg_path):
            try:
                load_config()
                check("load_config: malformed raises", False)
            except ValueError:
                check("load_config: malformed raises", True)

    print(f"\nResults: {passed} passed, {failed} failed\n")
    return failed


if __name__ == "__main__":
    failures = run_tests()
    sys.exit(1 if failures else 0)
