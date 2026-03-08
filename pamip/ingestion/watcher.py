"""
ingestion/watcher.py
Scans a configured directory for new eligible media files.
Designed for polling-based monitoring with an interface that supports
swapping in event-based monitoring (SR-1) without changing callers.
"""

from pathlib import Path


class FileWatcher:
    """
    FileWatcher
    Scans a watch directory for files matching the configured extensions.
    Returns only files not previously seen, preventing duplicate job creation.

    Duplicate prevention strategy: files are tracked by absolute path in
    memory. This is sufficient because completed jobs result in the processed
    file being moved out of the watch directory — it will not reappear on
    future scans. The seen set guards against duplicates within a single
    session (e.g. a file detected but not yet moved).

    Usage:
        watcher = FileWatcher(watch_dir="/media/incoming", allowed_extensions=[".mp4", ".mkv"])
        new_files = watcher.scan()  # returns list of Path objects
    """

    def __init__(self, watch_dir: str, allowed_extensions: list[str]):
        """
        __init__
        Args:
            watch_dir           (str)       — absolute path to the directory to monitor
            allowed_extensions  (list[str]) — file extensions to accept, e.g. [".mp4", ".mkv"]
        """
        self.watch_dir = Path(watch_dir)
        # Normalize extensions to lowercase for case-insensitive matching
        self.allowed_extensions = {ext.lower() for ext in allowed_extensions}
        self._seen: set[Path] = set()

    def scan(self) -> list[Path]:
        """
        scan
        Scans the watch directory for new eligible files.
        Returns a list of Path objects for files not previously returned.
        Returns an empty list if the directory does not exist or is empty.

        This method is the single integration point for SR-1 — an
        event-based subclass would override this method only.
        """
        if not self.watch_dir.exists():
            print(f"Warning: watch directory does not exist: {self.watch_dir}")
            return []

        new_files = []

        for path in self.watch_dir.iterdir():
            # Skip directories and already-seen files
            if not path.is_file():
                continue
            if path in self._seen:
                continue
            # FR-3: filter by configured extensions
            if path.suffix.lower() not in self.allowed_extensions:
                continue

            self._seen.add(path)
            new_files.append(path)

        return new_files
