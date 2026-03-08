"""
tools/executor.py
Runs external processes as pipeline steps.
Captures stdout/stderr, enforces timeouts, and handles graceful shutdown.
"""

import subprocess
import threading
import signal
from dataclasses import dataclass


# Seconds to wait after SIGTERM before forcing SIGKILL
SIGTERM_GRACE_PERIOD = 5


@dataclass
class ExecutionResult:
    """
    ExecutionResult
    Output of a single external process execution.

    Fields:
        success    (bool)      — True if exit code is 0
        exit_code  (int)       — process exit code; -1 if killed by timeout
        stdout     (str)       — captured standard output
        stderr     (str)       — captured standard error
        timed_out  (bool)      — True if the process was terminated due to timeout
    """
    success:   bool
    exit_code: int
    stdout:    str
    stderr:    str
    timed_out: bool


def _stream_output(stream, buffer: list, label: str):
    """
    _stream_output
    Reads lines from a subprocess stream, prints them to console,
    and appends them to a buffer for later capture.
    Intended to run in a dedicated thread.

    Args:
        stream  — readable stream from subprocess (stdout or stderr)
        buffer  (list) — shared list to accumulate output lines
        label   (str)  — prefix for console output, e.g. "[stdout]"
    """
    for line in iter(stream.readline, ""):
        stripped = line.rstrip()
        print(f"{label} {stripped}")
        buffer.append(line)
    stream.close()


def run_process(command: list[str], timeout_seconds: int) -> ExecutionResult:
    """
    run_process
    Executes an external command as a subprocess.
    Streams stdout and stderr to the console while also capturing them.
    Enforces a timeout — sends SIGTERM on expiry, then SIGKILL after
    a grace period if the process has not exited.

    Args:
        command          (list[str]) — command and arguments, e.g. ["ffmpeg", "-i", "input.mp4"]
        timeout_seconds  (int)       — maximum allowed runtime in seconds

    Returns:
        ExecutionResult — contains success flag, exit code, stdout, stderr, and timed_out flag
    """
    stdout_buf = []
    stderr_buf = []
    timed_out  = False

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,  # line-buffered for real-time streaming
    )

    # Stream stdout and stderr in separate threads so neither blocks the other
    stdout_thread = threading.Thread(
        target=_stream_output,
        args=(process.stdout, stdout_buf, "[stdout]"),
        daemon=True
    )
    stderr_thread = threading.Thread(
        target=_stream_output,
        args=(process.stderr, stderr_buf, "[stderr]"),
        daemon=True
    )
    stdout_thread.start()
    stderr_thread.start()

    try:
        process.wait(timeout=timeout_seconds)

    except subprocess.TimeoutExpired:
        timed_out = True
        print(f"[executor] Timeout reached ({timeout_seconds}s). Sending SIGTERM...")
        process.send_signal(signal.SIGTERM)

        try:
            # Give the process a chance to shut down cleanly
            process.wait(timeout=SIGTERM_GRACE_PERIOD)
            print("[executor] Process exited after SIGTERM.")
        except subprocess.TimeoutExpired:
            # Process ignored SIGTERM — force kill
            print("[executor] Process did not exit. Sending SIGKILL...")
            process.kill()
            process.wait()

    # Wait for output threads to flush before reading buffers
    stdout_thread.join()
    stderr_thread.join()

    exit_code = process.returncode if not timed_out else -1
    stdout    = "".join(stdout_buf)
    stderr    = "".join(stderr_buf)

    return ExecutionResult(
        success=   exit_code == 0,
        exit_code= exit_code,
        stdout=    stdout,
        stderr=    stderr,
        timed_out= timed_out,
    )
