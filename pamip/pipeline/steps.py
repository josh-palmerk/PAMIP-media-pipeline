"""
pipeline/steps.py
Step handler registry and individual step definitions.
Each handler is responsible for building the command that the executor
will run for its step.

File-type routing strategy:
    Rather than maintaining a separate pipeline config per file type,
    each handler checks the input extension and returns a no-op command
    if the file type is not applicable. This keeps the pipeline config
    simple and uniform while still doing the right thing per file type.

    Video steps: transcode, thumbnail
        Apply to: .mp4, .mov, .mkv
    Image steps: image_convert, image_compress
        Apply to: .png (convert only), .jpg/.jpeg (compress if over threshold)

To add a new step:
    1. Define a function:
           (file_path: str, output_dir: str, job: Job, options: dict) -> list[str]
    2. Decorate it with @register_step("your_step_name")
    3. Add the step name to the pipeline in config/config.json

No changes to core orchestration logic are required. (FR-12)
"""

from pathlib import Path
from jobs.models import Job


# ----------------------------------------
# Constants
# ----------------------------------------

# Extensions handled by video steps
_VIDEO_EXTS = {".mp4", ".mov", ".mkv"}

# Extensions handled by image steps
_IMAGE_EXTS = {".jpg", ".jpeg", ".png"}

# Fallback threshold used when compress_threshold_mb is not set in options
_DEFAULT_COMPRESS_THRESHOLD_MB = 2


# ----------------------------------------
# Registry
# ----------------------------------------

# Maps step_name strings to their handler functions
_STEP_REGISTRY: dict[str, callable] = {}


def register_step(step_name: str):
    """
    register_step
    Decorator that registers a step handler function under the given name.
    The decorated function must accept
        (file_path: str, output_dir: str, job: Job, options: dict)
    and return a list[str] representing the command to execute.

    Usage:
        @register_step("transcode")
        def handle_transcode(file_path, output_dir, job, options):
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
# Helpers
# ----------------------------------------

def _noop_command() -> list[str]:
    """
    _noop_command
    Returns a command that immediately exits with code 0.
    Used when a step is not applicable to the input file type.
    """
    return ["python", "-c", ""]


# ----------------------------------------
# Step Handlers
# ----------------------------------------

@register_step("transcode")
def handle_transcode(file_path: str, output_dir: str, job: Job, options: dict) -> list[str]:
    """
    handle_transcode
    Transcodes video files to H.264/AAC MP4. Handles .mp4 (re-encode
    to ensure H.264), .mov, and .mkv (container conversion + re-encode).
    Non-video files are skipped with a no-op command.

    Output is written to output_dir to prevent the watcher from
    picking it up as a new file.

    Args:
        file_path   (str)  — absolute path to the input file
        output_dir  (str)  — directory to write the transcoded file into
        job         (Job)  — the parent job
        options     (dict) — unused; present for interface consistency

    Returns:
        list[str] — command passed to run_process()

    FR-13
    """
    input_path = Path(file_path)

    if input_path.suffix.lower() not in _VIDEO_EXTS:
        return _noop_command()

    output_path = Path(output_dir) / (input_path.stem + "_transcoded.mp4")

    return [
        "ffmpeg",
        "-loglevel", "quiet",
        "-i",   str(input_path),
        "-c:v", "libx264",      # video codec: H.264
        "-c:a", "aac",          # audio codec: AAC
        "-y",                   # overwrite output without prompting
        str(output_path),
    ]


@register_step("thumbnail")
def handle_thumbnail(file_path: str, output_dir: str, job: Job, options: dict) -> list[str]:
    """
    handle_thumbnail
    Extracts a single JPEG frame from a video file at the 5-second mark.
    Non-video files are skipped with a no-op command.

    Args:
        file_path   (str)  — absolute path to the input file
        output_dir  (str)  — directory to write the thumbnail into
        job         (Job)  — the parent job
        options     (dict) — unused; present for interface consistency

    Returns:
        list[str] — command passed to run_process()
    """
    input_path = Path(file_path)

    if input_path.suffix.lower() not in _VIDEO_EXTS:
        return _noop_command()

    output_path = Path(output_dir) / (input_path.stem + "_thumbnail.jpg")

    return [
        "ffmpeg",
        "-loglevel", "quiet",
        "-i",       str(input_path),
        "-ss",      "00:00:05",     # seek to 5 seconds
        "-vframes", "1",            # capture one frame
        "-update",  "1",            # write single image (no sequence pattern)
        "-y",                       # overwrite output without prompting
        str(output_path),
    ]


@register_step("image_convert")
def handle_image_convert(file_path: str, output_dir: str, job: Job, options: dict) -> list[str]:
    """
    handle_image_convert
    Converts PNG files to JPEG format using ffmpeg.
    JPEGs and non-image files are skipped with a no-op command.

    Args:
        file_path   (str)  — absolute path to the input file
        output_dir  (str)  — directory to write the converted JPEG into
        job         (Job)  — the parent job
        options     (dict) — unused; present for interface consistency

    Returns:
        list[str] — command passed to run_process()
    """
    input_path = Path(file_path)

    if input_path.suffix.lower() != ".png":
        return _noop_command()

    output_path = Path(output_dir) / (input_path.stem + ".jpg")

    return [
        "ffmpeg",
        "-loglevel", "quiet",
        "-i",   str(input_path),
        "-q:v", "2",            # JPEG quality: 1 (best) – 31 (worst); 2 is near-lossless
        "-y",
        str(output_path),
    ]


@register_step("image_compress")
def handle_image_compress(file_path: str, output_dir: str, job: Job, options: dict) -> list[str]:
    """
    handle_image_compress
    Compresses JPEG files that exceed the configured size threshold.
    PNGs, videos, and small JPEGs are skipped with a no-op command.

    Compression uses ffmpeg with quality scale 4, which produces a
    noticeable size reduction while preserving acceptable visual quality.

    Args:
        file_path   (str)  — absolute path to the input file
        output_dir  (str)  — directory to write the compressed JPEG into
        job         (Job)  — the parent job
        options     (dict) — supports key:
                                compress_threshold_mb (int|float) — file size
                                threshold in megabytes; files at or below this
                                size are not compressed. Defaults to
                                _DEFAULT_COMPRESS_THRESHOLD_MB if not set.

    Returns:
        list[str] — command passed to run_process()
    """
    input_path = Path(file_path)

    if input_path.suffix.lower() not in {".jpg", ".jpeg"}:
        return _noop_command()

    # Read threshold from step options; fall back to module default
    threshold_mb    = options.get("compress_threshold_mb", _DEFAULT_COMPRESS_THRESHOLD_MB)
    threshold_bytes = threshold_mb * 1024 * 1024

    if input_path.stat().st_size <= threshold_bytes:
        return _noop_command()

    output_path = Path(output_dir) / (input_path.stem + "_compressed.jpg")

    return [
        "ffmpeg",
        "-loglevel", "quiet",
        "-i",   str(input_path),
        "-q:v", "4",            # slightly more compression than image_convert
        "-y",
        str(output_path),
    ]
