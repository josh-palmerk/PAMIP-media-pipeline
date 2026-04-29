"""
test_steps.py
Tests for the pipeline step registry and the file-type routing logic
in pipeline/steps.py. Verifies:
  - register_step / get_step_handler round-trip
  - each step handler returns a no-op when the input file type is
    outside its declared support set
  - each step handler returns an ffmpeg command when the input matches
  - image_compress threshold logic skips small files

Run from the project root:
    python -m tests.test_steps
"""

import sys
import tempfile
from pathlib import Path


def run_tests():
    from pipeline.steps import (
        register_step,
        get_step_handler,
        handle_transcode,
        handle_thumbnail,
        handle_image_convert,
        handle_image_compress,
    )

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

    # Real ffmpeg never runs in these tests; we only inspect the command
    # the handler builds. Job is unused by every handler today, so None is fine.
    JOB = None

    def is_noop(cmd: list) -> bool:
        """A no-op command runs Python with empty code and exits 0."""
        return len(cmd) >= 3 and cmd[1] == "-c" and cmd[2] == ""

    def is_ffmpeg(cmd: list) -> bool:
        return len(cmd) > 0 and cmd[0] == "ffmpeg"

    # ----------------------------------------
    # Registry
    # ----------------------------------------
    print("\n--- Registry ---")

    # All four production handlers should be registered
    for name in ("transcode", "thumbnail", "image_convert", "image_compress"):
        try:
            handler = get_step_handler(name)
            check(f"registry: '{name}' is registered", callable(handler))
        except KeyError:
            check(f"registry: '{name}' is registered", False)

    # Unknown step raises KeyError
    try:
        get_step_handler("nonexistent_step")
        check("registry: unknown step raises KeyError", False)
    except KeyError:
        check("registry: unknown step raises KeyError", True)

    # register_step round-trip
    @register_step("__test_handler__")
    def _test_handler(file_path, output_dir, job, options):
        return ["echo", "hello"]

    looked_up = get_step_handler("__test_handler__")
    check("register_step: handler retrievable", looked_up is _test_handler)
    check("register_step: handler still callable",
          looked_up("p", "o", None, {}) == ["echo", "hello"])

    # ----------------------------------------
    # transcode — video only
    # ----------------------------------------
    print("\n--- transcode ---")

    for ext in (".mp4", ".mov", ".mkv"):
        cmd = handle_transcode(f"/media/clip{ext}", "/out", JOB, {})
        check(f"transcode {ext}: emits ffmpeg",         is_ffmpeg(cmd))
        # The output path is built via pathlib, so it'll use the host OS's
        # separator (\ on Windows, / on Linux). Compare via Path equality.
        out_arg = Path(cmd[-1])
        check(f"transcode {ext}: writes to output_dir",  out_arg.parent == Path("/out"))

    for ext in (".jpg", ".png", ".pdf", ".txt"):
        cmd = handle_transcode(f"/media/file{ext}", "/out", JOB, {})
        check(f"transcode {ext}: no-op",                is_noop(cmd))

    # Case-insensitivity: a .MP4 should be treated as a video
    cmd = handle_transcode("/media/UPPER.MP4", "/out", JOB, {})
    check("transcode .MP4 (uppercase): emits ffmpeg",   is_ffmpeg(cmd))

    # ----------------------------------------
    # thumbnail — video only
    # ----------------------------------------
    print("\n--- thumbnail ---")

    cmd = handle_thumbnail("/media/clip.mp4", "/out", JOB, {})
    check("thumbnail .mp4: emits ffmpeg",               is_ffmpeg(cmd))
    check("thumbnail: outputs .jpg",                    any(s.endswith(".jpg") for s in cmd))

    for ext in (".jpg", ".png"):
        cmd = handle_thumbnail(f"/media/img{ext}", "/out", JOB, {})
        check(f"thumbnail {ext}: no-op",               is_noop(cmd))

    # ----------------------------------------
    # image_convert — png only
    # ----------------------------------------
    print("\n--- image_convert ---")

    cmd = handle_image_convert("/media/img.png", "/out", JOB, {})
    check("image_convert .png: emits ffmpeg",           is_ffmpeg(cmd))
    check("image_convert: outputs .jpg",                any(s.endswith(".jpg") for s in cmd))

    for ext in (".jpg", ".jpeg", ".mp4", ".mov"):
        cmd = handle_image_convert(f"/media/file{ext}", "/out", JOB, {})
        check(f"image_convert {ext}: no-op",           is_noop(cmd))

    # ----------------------------------------
    # image_compress — jpeg above threshold only
    # ----------------------------------------
    print("\n--- image_compress ---")

    # Non-JPEGs are no-op without inspecting size
    for ext in (".png", ".mp4", ".mov", ".mkv"):
        cmd = handle_image_compress(f"/media/file{ext}", "/out", JOB, {})
        check(f"image_compress {ext}: no-op",          is_noop(cmd))

    # JPEGs need real files because the handler checks file size on disk
    with tempfile.TemporaryDirectory() as tmp:
        small_jpg = Path(tmp) / "small.jpg"
        small_jpg.write_bytes(b"x" * 1024)            # 1 KB — below default threshold
        cmd = handle_image_compress(str(small_jpg), tmp, JOB, {})
        check("image_compress small jpg: no-op (under threshold)", is_noop(cmd))

        large_jpg = Path(tmp) / "large.jpg"
        large_jpg.write_bytes(b"x" * (3 * 1024 * 1024))   # 3 MB — above default 2 MB threshold
        cmd = handle_image_compress(str(large_jpg), tmp, JOB, {})
        check("image_compress large jpg: emits ffmpeg",            is_ffmpeg(cmd))

        # Custom threshold from options — set high so the 3MB file falls under
        cmd = handle_image_compress(
            str(large_jpg), tmp, JOB,
            {"compress_threshold_mb": 10}
        )
        check("image_compress: custom threshold respected (skips when under)",
              is_noop(cmd))

        # Custom threshold from options — set very low so even small file compresses
        cmd = handle_image_compress(
            str(small_jpg), tmp, JOB,
            {"compress_threshold_mb": 0}
        )
        check("image_compress: custom threshold respected (compresses when over)",
              is_ffmpeg(cmd))

        # .jpeg variant also handled
        jpeg_file = Path(tmp) / "photo.jpeg"
        jpeg_file.write_bytes(b"x" * (3 * 1024 * 1024))
        cmd = handle_image_compress(str(jpeg_file), tmp, JOB, {})
        check("image_compress .jpeg variant: emits ffmpeg",        is_ffmpeg(cmd))

    print(f"\nResults: {passed} passed, {failed} failed\n")
    return failed


if __name__ == "__main__":
    failures = run_tests()
    sys.exit(1 if failures else 0)
