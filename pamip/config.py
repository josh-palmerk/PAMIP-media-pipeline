"""
config.py
Loads, validates, and provides access to PAMIP runtime configuration.
Reads from config/config.json; creates a default file if none exists.
"""

import json
from pathlib import Path
from dataclasses import dataclass, field


CONFIG_PATH = Path("config/config.json")

# Default configuration written to disk if no config file is found
DEFAULT_CONFIG = {
    "watch_directory": "./media/incoming",
    "output_directory": "./media/processed",
    "allowed_extensions": [".mp4", ".mkv", ".jpg", ".png"],
    "poll_interval_seconds": 5,
    "max_concurrent_jobs": 1,
    "pipeline": [
        {"step_name": "transcode", "max_attempts": 3, "timeout_seconds": 300},
        {"step_name": "thumbnail", "max_attempts": 2, "timeout_seconds": 60},
    ]
}


@dataclass
class StepConfig:
    """
    StepConfig
    Configuration for a single pipeline step.

    Fields:
        step_name        (str) — unique name matching a registered step handler
        max_attempts     (int) — maximum execution attempts before failure
        timeout_seconds  (int) — seconds before the step is forcibly terminated
    """
    step_name:       str
    max_attempts:    int
    timeout_seconds: int


@dataclass
class Config:
    """
    Config
    Top-level application configuration.

    Fields:
        watch_directory        (str)              — directory to monitor for new files
        output_directory       (str)              — directory where processed files are moved
        allowed_extensions     (list[str])        — file extensions that trigger job creation
        poll_interval_seconds  (int)              — seconds between directory scans
        max_concurrent_jobs    (int)              — number of jobs to process simultaneously
        pipeline               (list[StepConfig]) — ordered list of pipeline step definitions
    """
    watch_directory:       str
    output_directory:      str
    allowed_extensions:    list[str]
    poll_interval_seconds: int
    max_concurrent_jobs:   int
    pipeline:              list[StepConfig] = field(default_factory=list)


def _write_default_config():
    """
    _write_default_config
    Creates the config directory and writes DEFAULT_CONFIG to disk.
    Called automatically when no config file is found on startup.
    """
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_PATH, "w") as f:
        json.dump(DEFAULT_CONFIG, f, indent=4)
    print(f"No config found. Default config created at: {CONFIG_PATH}")


def _validate(raw: dict):
    """
    _validate
    Checks that required top-level keys are present and that the pipeline
    list is non-empty. Raises ValueError on any violation.
    """
    required_keys = [
        "watch_directory", "output_directory", "allowed_extensions",
        "poll_interval_seconds", "max_concurrent_jobs", "pipeline"
    ]
    for key in required_keys:
        if key not in raw:
            raise ValueError(f"Missing required config key: '{key}'")

    if not isinstance(raw["pipeline"], list) or len(raw["pipeline"]) == 0:
        raise ValueError("Config 'pipeline' must be a non-empty list of step definitions.")

    for i, step in enumerate(raw["pipeline"]):
        for step_key in ["step_name", "max_attempts", "timeout_seconds"]:
            if step_key not in step:
                raise ValueError(f"Pipeline step {i} missing required key: '{step_key}'")


def load_config() -> Config:
    """
    load_config
    Reads and parses the config file, creating a default if missing.
    Returns a Config dataclass instance.
    Raises ValueError if the config file is malformed or missing required fields.
    """
    if not CONFIG_PATH.exists():
        _write_default_config()

    with open(CONFIG_PATH, "r") as f:
        raw = json.load(f)

    _validate(raw)

    pipeline = [
        StepConfig(
            step_name=      step["step_name"],
            max_attempts=   step["max_attempts"],
            timeout_seconds=step["timeout_seconds"],
        )
        for step in raw["pipeline"]
    ]

    return Config(
        watch_directory=        raw["watch_directory"],
        output_directory=       raw["output_directory"],
        allowed_extensions=     raw["allowed_extensions"],
        poll_interval_seconds=  raw["poll_interval_seconds"],
        max_concurrent_jobs=    raw["max_concurrent_jobs"],
        pipeline=               pipeline,
    )
