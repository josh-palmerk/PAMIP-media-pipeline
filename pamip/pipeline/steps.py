"""
pipeline/steps.py
Step handler registry and individual step definitions.
Each handler is responsible for building the command that the executor
will run for its step.

To add a new step:
    1. Define a function that accepts (file_path: str, job: Job) -> list[str]
    2. Decorate it with @register_step("your_step_name")
    3. Add the step name to the pipeline in config/config.json

No changes to core orchestration logic are required. (FR-12)
"""

from jobs.models import Job


# ----------------------------------------
# Registry
# ----------------------------------------

# Maps step_name strings to their handler functions
_STEP_REGISTRY: dict[str, callable] = {}


def register_step(step_name: str):
    """
    register_step
    Decorator that registers a step handler function under the given name.
    The decorated function must accept (file_path: str, job: Job) and
    return a list[str] representing the command to execute.

    Usage:
        @register_step("transcode")
        def handle_transcode(file_path: str, job: Job) -> list[str]:
            return ["ffmpeg", "-i", file_path, ...]
    """
    def decorator(fn):
        _STEP_REGISTRY[step_name] = fn
        return fn
    return decorator


def get_step_handler(step_name: str) -> callable:
    """
    get_step_handler
    Looks up and returns the handler function for the given step name.
    Raises KeyError if no handler is registered for that name.

    Args:
        step_name (str) — name matching a registered step

    Returns:
        callable — the handler function for that step
    """
    if step_name not in _STEP_REGISTRY:
        raise KeyError(f"No handler registered for step: '{step_name}'")
    return _STEP_REGISTRY[step_name]


# ----------------------------------------
# Step Handlers
# ----------------------------------------

@register_step("transcode")
def handle_transcode(file_path: str, job: Job) -> list[str]:
    """
    handle_transcode
    Builds an ffmpeg command to transcode the input file to H.264/AAC MP4.
    Output file is written alongside the input with a '_transcoded' suffix.

    Args:
        file_path  (str) — absolute path to the input media file
        job        (Job) — the parent job (available for context if needed)

    Returns:
        list[str] — command passed to run_process()
    """
    from pathlib import Path
    input_path  = Path(file_path)
    output_path = input_path.with_stem(input_path.stem + "_transcoded").with_suffix(".mp4")

    return [
        "ffmpeg",
        "-i",       str(input_path),
        "-c:v",     "libx264",      # video codec
        "-c:a",     "aac",          # audio codec
        "-y",                       # overwrite output without prompting
        str(output_path),
    ]


@register_step("thumbnail")
def handle_thumbnail(file_path: str, job: Job) -> list[str]:
    """
    handle_thumbnail
    Builds an ffmpeg command to extract a single frame as a JPEG thumbnail.
    Frame is captured at the 5-second mark. Output is written alongside
    the input with a '_thumbnail' suffix.

    Args:
        file_path  (str) — absolute path to the input media file
        job        (Job) — the parent job (available for context if needed)

    Returns:
        list[str] — command passed to run_process()
    """
    from pathlib import Path
    input_path  = Path(file_path)
    output_path = input_path.with_stem(input_path.stem + "_thumbnail").with_suffix(".jpg")

    return [
        "ffmpeg",
        "-i",       str(input_path),
        "-ss",      "00:00:05",     # seek to 5 seconds
        "-vframes", "1",            # capture one frame
        "-y",                       # overwrite output without prompting
        str(output_path),
    ]
