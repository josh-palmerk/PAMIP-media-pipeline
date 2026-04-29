"""
test_watcher.py
Tests for FileWatcher in ingestion/watcher.py.
Uses a temporary directory to simulate the watch directory.

Run from the project root:
    python test_watcher.py
"""

import sys
import tempfile
from pathlib import Path


def run_tests():
    from ingestion.watcher import FileWatcher

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
    # Helpers
    # ----------------------------------------

    def make_watcher(tmp_dir, extensions=None):
        if extensions is None:
            extensions = [".mp4", ".mkv", ".jpg", ".png"]
        return FileWatcher(watch_dir=str(tmp_dir), allowed_extensions=extensions)

    def touch(directory, filename) -> Path:
        """Create an empty file and return its Path."""
        p = Path(directory) / filename
        p.touch()
        return p

    # ----------------------------------------
    # FileWatcher Tests
    # ----------------------------------------
    print("\n--- FileWatcher ---")

    # Empty directory returns nothing
    with tempfile.TemporaryDirectory() as tmp:
        watcher = make_watcher(tmp)
        check("empty directory returns empty list",  watcher.scan() == [])

    # Supported file is detected — FR-3
    with tempfile.TemporaryDirectory() as tmp:
        watcher = make_watcher(tmp)
        touch(tmp, "video.mp4")
        results = watcher.scan()
        check("supported file detected",             len(results) == 1)
        check("result is a Path",                    isinstance(results[0], Path))
        check("correct filename returned",           results[0].name == "video.mp4")

    # Unsupported file is ignored — FR-3
    with tempfile.TemporaryDirectory() as tmp:
        watcher = make_watcher(tmp)
        touch(tmp, "document.pdf")
        touch(tmp, "archive.zip")
        check("unsupported files ignored",           watcher.scan() == [])

    # Mixed files — only supported ones returned — FR-3
    with tempfile.TemporaryDirectory() as tmp:
        watcher = make_watcher(tmp)
        touch(tmp, "video.mp4")
        touch(tmp, "image.jpg")
        touch(tmp, "document.pdf")
        results = watcher.scan()
        names = {r.name for r in results}
        check("mixed: correct count returned",       len(results) == 2)
        check("mixed: mp4 included",                 "video.mp4" in names)
        check("mixed: jpg included",                 "image.jpg" in names)
        check("mixed: pdf excluded",                 "document.pdf" not in names)

    # Already-seen files not returned again — FR-2
    with tempfile.TemporaryDirectory() as tmp:
        watcher = make_watcher(tmp)
        touch(tmp, "video.mp4")
        first_scan = watcher.scan()
        second_scan = watcher.scan()
        check("first scan detects file",             len(first_scan) == 1)
        check("second scan skips seen file",         second_scan == [])

    # New file appears on subsequent scan
    with tempfile.TemporaryDirectory() as tmp:
        watcher = make_watcher(tmp)
        touch(tmp, "first.mp4")
        watcher.scan()
        touch(tmp, "second.mkv")
        results = watcher.scan()
        check("new file detected on second scan",    len(results) == 1)
        check("correct new file returned",           results[0].name == "second.mkv")

    # Moved file reappears after being re-added (simulates crash recovery)
    with tempfile.TemporaryDirectory() as tmp:
        watcher = make_watcher(tmp)
        p = touch(tmp, "video.mp4")
        watcher.scan()
        p.unlink()          # simulate file being moved/deleted
        p.touch()           # simulate re-appearance after crash
        results = watcher.scan()
        check("re-appeared file not re-detected in same session", results == [])

    # Extension matching is case-insensitive — FR-3
    with tempfile.TemporaryDirectory() as tmp:
        watcher = make_watcher(tmp)
        touch(tmp, "VIDEO.MP4")
        touch(tmp, "image.JPG")
        results = watcher.scan()
        check("uppercase extensions accepted",       len(results) == 2)

    # Subdirectories are not scanned — proven by including a sibling top-level
    # file: if recursion ever leaks in, the count would be 2, not 1.
    with tempfile.TemporaryDirectory() as tmp:
        watcher = make_watcher(tmp)
        subdir = Path(tmp) / "subdir"
        subdir.mkdir()
        (subdir / "nested.mp4").touch()
        touch(tmp, "top.mp4")
        results = watcher.scan()
        names = {r.name for r in results}
        check("subdir test: top-level file detected", "top.mp4" in names)
        check("subdir test: nested file ignored",     "nested.mp4" not in names)
        check("subdir test: exactly one file",        len(results) == 1)

    # Non-existent watch directory returns empty list gracefully
    watcher = FileWatcher(watch_dir="/nonexistent/path", allowed_extensions=[".mp4"])
    check("missing watch dir returns empty list",    watcher.scan() == [])

    print(f"\nResults: {passed} passed, {failed} failed\n")
    return failed


if __name__ == "__main__":
    failures = run_tests()
    sys.exit(1 if failures else 0)
