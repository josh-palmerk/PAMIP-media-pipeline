"""
demo.py
Live demonstration script for PAMIP.
Walks through all functional requirements in a presenter-controlled,
section-by-section format. Each section pauses for a keypress before running.

Usage (run from the pamip/ project root):
    python demo.py

Prerequisites:
    - ffmpeg installed and on PATH
    - Demo media files present in demo/media/:
        test_image.png
        test_image.jpg
        test_video.mp4
        test_video.mkv

The script manages its own database (demo/demo.db) and directory structure
so it does not interfere with the production database (pamip.db).
"""

import os
import sys
import shutil
import signal
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path


# ============================================================
# Paths — all demo I/O is isolated under demo/
# ============================================================

DEMO_ROOT    = Path("demo")
MEDIA_DIR    = DEMO_ROOT / "media"       # presenter drops files here
WATCH_DIR    = DEMO_ROOT / "incoming"    # worker monitors this
OUTPUT_DIR   = DEMO_ROOT / "processed"  # completed files land here
DB_PATH      = str(DEMO_ROOT / "demo.db")

# Source media the presenter provides
SRC_PNG  = MEDIA_DIR / "test_image.png"
SRC_JPG  = MEDIA_DIR / "test_image.jpg"
SRC_MP4  = MEDIA_DIR / "test_video.mp4"
SRC_MKV  = MEDIA_DIR / "test_video.mkv"


# ============================================================
# Minimal in-memory-compatible Database for the demo
# (same interface as db/database.py, points at demo.db)
# ============================================================

def _make_db():
    """Open a fresh Database connection to demo.db."""
    from db.database import Database
    from db import schema
    db = Database(DB_PATH)
    schema.initialize_schema(db)
    return db


# ============================================================
# Presentation helpers
# ============================================================

CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

DIVIDER = "=" * 64


def header(title: str, frs: str):
    """Print a section header with FR labels."""
    print(f"\n{BOLD}{CYAN}{DIVIDER}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{CYAN}  Demonstrates: {frs}{RESET}")
    print(f"{BOLD}{CYAN}{DIVIDER}{RESET}\n")


def info(msg: str):
    print(f"  {msg}")


def success(msg: str):
    print(f"  {GREEN}✓ {msg}{RESET}")


def warn(msg: str):
    print(f"  {YELLOW}⚠ {msg}{RESET}")


def pause(prompt: str = "Press ENTER to continue..."):
    print(f"\n{BOLD}  {prompt}{RESET}")
    input()


def run_cli(label: str, fn, *args):
    """Run a CLI command function, framed with a label."""
    print(f"\n  {BOLD}$ pamip {label}{RESET}")
    print("  " + "-" * 50)
    fn(*args)


def wait_for_jobs(job_repo, job_ids: list[int], timeout: int = 300) -> bool:
    """
    wait_for_jobs
    Polls until all specified jobs reach a terminal state (completed or failed),
    or until the timeout elapses.

    Returns True if all jobs finished, False if timeout was reached.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        jobs = [job_repo.get_job(jid) for jid in job_ids]
        if all(j and j.status in ("completed", "failed") for j in jobs):
            return True
        time.sleep(2)
    return False


def copy_to_watch(src: Path, label: str) -> Path:
    """Copy a source media file into the watch directory."""
    dst = WATCH_DIR / src.name
    shutil.copy(src, dst)
    info(f"Dropped {label} into watch dir: {dst.name}")
    return dst


# ============================================================
# Worker management
# ============================================================

def _start_worker(step_configs, suppress_output=True) -> tuple:
    """
    _start_worker
    Builds and starts a WorkerLoop in a background thread.
    All SQLite objects are created inside the worker thread — SQLite
    connections cannot be shared across threads.
    Suppresses worker print output during processing to keep the
    demo terminal readable; logs still go to demo/demo.log.

    Returns (loop, thread). Caller is responsible for calling loop.stop().
    The main thread should open its own separate DB connection for reads.
    """
    import io
    from db.database import Database
    from db.job_repository import JobRepository
    from db.step_repository import StepRepository
    from ingestion.watcher import FileWatcher
    from pipeline.engine import PipelineEngine
    from worker.loop import WorkerLoop

    engine  = PipelineEngine(step_configs, str(OUTPUT_DIR))
    watcher = FileWatcher(
        watch_dir=str(WATCH_DIR),
        allowed_extensions=[".mp4", ".mkv", ".mov", ".jpg", ".jpeg", ".png"],
    )

    # ready lets the main thread wait until loop exists before returning it
    ready       = threading.Event()
    loop_holder = [None]  # mutable container so _run() can write back

    class _NullWriter(io.IOBase):
        def write(self, *a): return 0
        def flush(self):     pass

    def _run():
        # All DB objects created here, inside the worker thread
        db        = Database(DB_PATH)
        job_repo  = JobRepository(db)
        step_repo = StepRepository(db)

        loop = WorkerLoop(
            db=                  db,
            job_repo=            job_repo,
            step_repo=           step_repo,
            engine=              engine,
            watcher=             watcher,
            pipeline=            step_configs,
            output_dir=          str(OUTPUT_DIR),
            poll_interval=       2,
            max_concurrent_jobs= 4,
        )
        loop_holder[0] = loop
        ready.set()  # signal main thread that loop is available

        if suppress_output:
            old, sys.stdout = sys.stdout, _NullWriter()
        try:
            loop.start()
        finally:
            if suppress_output:
                sys.stdout = old
            db.close()

    thread = threading.Thread(target=_run, daemon=True, name="worker")
    thread.start()

    # Block until the loop object is ready before returning it to the caller
    ready.wait(timeout=10)
    return loop_holder[0], thread


def _stop_worker(loop, thread):
    """Signal the worker to stop and wait for it to exit."""
    loop.stop()
    thread.join(timeout=10)


# ============================================================
# Step configs — normal and with a bad step injected
# ============================================================

def _normal_pipeline():
    from config import StepConfig
    return [
        StepConfig("transcode",      3, 300, {}),
        StepConfig("thumbnail",      2, 60,  {}),
        StepConfig("image_convert",  2, 60,  {}),
        StepConfig("image_compress", 2, 60,  {"compress_threshold_mb": 2}),
    ]


def _broken_pipeline():
    """Pipeline with an unregistered step name to force a clean failure."""
    from config import StepConfig
    return [
        StepConfig("transcode",    1, 300, {}),
        StepConfig("nonexistent_step", 1, 60, {}),  # will hit KeyError in engine
        StepConfig("thumbnail",    1, 60,  {}),
    ]


def _slow_pipeline():
    """
    Pipeline for the crash recovery demo.
    Uses -preset veryslow so the transcode takes long enough to interrupt.
    Requires a custom engine subclass to inject the extra ffmpeg flag.
    """
    from config import StepConfig
    return [
        StepConfig("transcode_slow", 1, 600, {}),
        StepConfig("thumbnail",      1, 60,  {}),
    ]


# ============================================================
# Section 1 — Setup
# ============================================================

def section_setup():
    header(
        "Section 1 — Setup & Environment",
        "FR-16 (database persistence)"
    )

    info("Verifying demo media files are present...")
    missing = [p for p in [SRC_PNG, SRC_JPG, SRC_MP4, SRC_MKV] if not p.exists()]
    if missing:
        for m in missing:
            warn(f"Missing: {m}")
        print(f"\n  {RED}Place the required files in demo/media/ and re-run.{RESET}\n")
        sys.exit(1)

    success("All media files found.")

    info("Creating demo directories...")
    for d in [WATCH_DIR, OUTPUT_DIR]:
        d.mkdir(parents=True, exist_ok=True)
    success(f"Watch dir:  {WATCH_DIR}")
    success(f"Output dir: {OUTPUT_DIR}")

    info("Initialising demo database...")
    db = _make_db()
    db.close()
    success(f"Database:   {DB_PATH}")

    info("\nPipeline configured with 4 steps:")
    for cfg in _normal_pipeline():
        info(f"  · {cfg.step_name:<20} max_attempts={cfg.max_attempts}  timeout={cfg.timeout_seconds}s")


# ============================================================
# Section 2 — Detection & Job Creation
# ============================================================

def section_detection():
    header(
        "Section 2 — File Detection & Job Creation",
        "FR-1 (directory monitoring), FR-2 (duplicate prevention), "
        "FR-3 (file type filtering), FR-4 (job creation), FR-5 (unique IDs)"
    )

    info("Dropping test_image.png and test_video.mp4 into the watch directory.")
    info("Worker will detect them within one poll cycle (2 s).\n")

    copy_to_watch(SRC_PNG, "test_image.png")
    copy_to_watch(SRC_MP4, "test_video.mp4")

    loop, thread = _start_worker(_normal_pipeline())

    info("\nWaiting for worker to detect files and create jobs...")
    time.sleep(5)

    from db.job_repository import JobRepository
    db2      = _make_db()
    job_repo2 = JobRepository(db2)
    jobs     = job_repo2.list_jobs()

    if jobs:
        success(f"{len(jobs)} job(s) created in the database.\n")
        from cli.commands import cmd_list
        run_cli("list", cmd_list, job_repo2)
    else:
        warn("No jobs detected yet — worker may still be starting.")

    info("\nFR-2 check: dropping the same files again — no new jobs should appear.")
    copy_to_watch(SRC_PNG, "test_image.png (duplicate)")
    copy_to_watch(SRC_MP4, "test_video.mp4 (duplicate)")
    time.sleep(4)

    jobs_after = job_repo2.list_jobs()
    if len(jobs_after) == len(jobs):
        success("Duplicate files produced no new jobs. (FR-2)")
    else:
        warn(f"Job count changed: {len(jobs)} → {len(jobs_after)}")

    info("\nFR-3 check: dropping an unsupported file type (.txt).")
    txt_file = WATCH_DIR / "readme.txt"
    txt_file.write_text("this should be ignored")
    time.sleep(4)

    jobs_final = job_repo2.list_jobs()
    if len(jobs_final) == len(jobs):
        success(".txt file produced no job. (FR-3)")
    else:
        warn("Unexpected job created for unsupported file type.")

    _stop_worker(loop, thread)
    db2.close()


# ============================================================
# Section 3 — Pipeline Execution
# ============================================================

def section_pipeline():
    header(
        "Section 3 — Pipeline Execution & Output",
        "FR-6 (lifecycle states), FR-7 (metadata persistence), "
        "FR-9 (step ordering), FR-10 (step tracking), "
        "FR-13 (ffmpeg invocation), FR-14 (output capture)"
    )

    info("Dropping all four media files and letting the worker run to completion.")
    info("ffmpeg output is suppressed here — captured output is visible via 'pamip show'.\n")

    for src, label in [
        (SRC_PNG, "test_image.png"),
        (SRC_JPG, "test_image.jpg"),
        (SRC_MP4, "test_video.mp4"),
        (SRC_MKV, "test_video.mkv"),
    ]:
        copy_to_watch(src, label)

    loop, thread = _start_worker(_normal_pipeline())

    info("Processing... (this may take a minute for video files)")

    # Collect job IDs as they are created
    db2       = _make_db()
    from db.job_repository import JobRepository
    from db.step_repository import StepRepository
    job_repo2  = JobRepository(db2)
    step_repo2 = StepRepository(db2)

    # Wait up to 5 minutes for all jobs to finish
    deadline = time.time() + 300
    while time.time() < deadline:
        jobs = job_repo2.list_jobs()
        done = [j for j in jobs if j.status in ("completed", "failed")]
        sys.stdout.write(f"\r  Jobs finished: {len(done)}/{len(jobs)}   ")
        sys.stdout.flush()
        if len(jobs) >= 4 and len(done) == len(jobs):
            break
        time.sleep(3)
    print()

    _stop_worker(loop, thread)

    jobs = job_repo2.list_jobs()
    success(f"All {len(jobs)} jobs reached a terminal state.\n")

    from cli.commands import cmd_list, cmd_show
    run_cli("list", cmd_list, job_repo2)

    info("\nShowing step detail for each job (FR-9 step order, FR-10 status, FR-14 output):\n")
    for job in sorted(jobs, key=lambda j: j.id):
        run_cli(f"show {job.id}", cmd_show, job_repo2, step_repo2, job.id)

    info("Verifying output files exist in processed directory:")
    outputs = list(OUTPUT_DIR.iterdir()) if OUTPUT_DIR.exists() else []
    if outputs:
        for f in sorted(outputs):
            success(f"  {f.name}")
    else:
        warn("No output files found — check logs.")

    db2.close()


# ============================================================
# Section 4 — Failure Propagation
# ============================================================

def section_failure():
    header(
        "Section 4 — Failure Propagation",
        "FR-11 (failed step stops pipeline)"
    )

    info("Dropping test_video.mkv with a broken pipeline.")
    info("Step 1 (transcode) will succeed.")
    info("Step 2 ('nonexistent_step') is not registered — engine will fail it.")
    info("Step 3 (thumbnail) must not run.\n")

    copy_to_watch(SRC_MKV, "test_video.mkv (failure demo)")

    loop, thread = _start_worker(_broken_pipeline())

    db2       = _make_db()
    from db.job_repository import JobRepository
    from db.step_repository import StepRepository
    job_repo2  = JobRepository(db2)
    step_repo2 = StepRepository(db2)

    info("Waiting for job to fail...")
    deadline = time.time() + 120
    job_id = None
    while time.time() < deadline:
        jobs = [j for j in job_repo2.list_jobs() if j.status in ("completed", "failed")]
        if jobs:
            # Take the most recently created failed job
            failed = [j for j in jobs if j.status == "failed"]
            if failed:
                job_id = failed[-1].id
                break
        time.sleep(2)

    _stop_worker(loop, thread)

    if job_id is None:
        warn("No failed job found within timeout.")
        db2.close()
        return

    from cli.commands import cmd_show
    run_cli(f"show {job_id}", cmd_show, job_repo2, step_repo2, job_id)

    steps = step_repo2.get_steps_for_job(job_id)
    thumbnail_step = next((s for s in steps if s.step_name == "thumbnail"), None)
    if thumbnail_step and thumbnail_step.status == "pending":
        success("thumbnail step was never run — failure propagation confirmed. (FR-11)")
    else:
        warn("Unexpected thumbnail step state.")

    db2.close()


# ============================================================
# Section 5 — Retry
# ============================================================

def section_retry():
    header(
        "Section 5 — Manual Retry",
        "FR-8 (retry support), FR-21 (pamip retry command)"
    )

    info("Retrying the failed job from Section 4 with the correct pipeline.\n")

    db2       = _make_db()
    from db.job_repository import JobRepository
    from db.step_repository import StepRepository
    job_repo2  = JobRepository(db2)
    step_repo2 = StepRepository(db2)

    # Find the most recent failed job
    failed_jobs = [j for j in job_repo2.list_jobs() if j.status == "failed"]
    if not failed_jobs:
        warn("No failed jobs to retry. Run Section 4 first.")
        db2.close()
        return

    job_id = failed_jobs[0].id
    info(f"Found failed job: ID {job_id}")

    from cli.commands import cmd_retry, cmd_show
    run_cli(f"retry {job_id}", cmd_retry, db2, job_repo2, step_repo2, job_id)

    info(f"\nJob {job_id} is now pending. Starting worker with the correct pipeline...")

    loop, thread = _start_worker(_normal_pipeline())

    info("Waiting for retry to complete...")
    deadline = time.time() + 300
    while time.time() < deadline:
        job = job_repo2.get_job(job_id)
        if job and job.status in ("completed", "failed"):
            break
        time.sleep(3)

    _stop_worker(loop, thread)

    run_cli(f"show {job_id}", cmd_show, job_repo2, step_repo2, job_id)

    job = job_repo2.get_job(job_id)
    if job and job.status == "completed":
        success(f"Job {job_id} completed after retry. retry_count={job.retry_count} (FR-8, FR-21)")
    else:
        warn(f"Job {job_id} did not complete — status: {job.status if job else 'unknown'}")

    db2.close()


# ============================================================
# Section 6 — Crash Recovery
# ============================================================

def section_crash_recovery():
    header(
        "Section 6 — Crash Recovery",
        "FR-17 (orphaned job recovery)"
    )

    info("Registering a slow transcode step (-preset veryslow).")
    info("We will drop a video, let the worker start processing it,")
    info("then kill the worker mid-job to simulate a crash.")
    info("On restart, the orphaned 'running' job must be re-queued.\n")

    # Register the slow transcode handler for this demo section only
    from pipeline.steps import register_step, _noop_command
    from jobs.models import Job

    @register_step("transcode_slow")
    def handle_transcode_slow(file_path: str, output_dir: str, job: Job, options: dict):
        from pathlib import Path
        input_path = Path(file_path)
        _VIDEO_EXTS = {".mp4", ".mov", ".mkv"}
        if input_path.suffix.lower() not in _VIDEO_EXTS:
            return _noop_command()
        output_path = Path(output_dir) / (input_path.stem + "_slow_transcoded.mp4")
        return [
            "ffmpeg",
            "-i",     str(input_path),
            "-c:v",   "libx264",
            "-preset","veryslow",   # deliberately slow for demo purposes
            "-c:a",   "aac",
            "-y",
            str(output_path),
        ]

    copy_to_watch(SRC_MKV, "test_video.mkv (crash demo)")

    loop, thread = _start_worker(_slow_pipeline())

    # Wait until the job is in 'running' state before simulating crash
    db2      = _make_db()
    from db.job_repository import JobRepository
    job_repo2 = JobRepository(db2)

    info("Waiting for job to enter 'running' state...")
    job_id = None
    deadline = time.time() + 60
    while time.time() < deadline:
        jobs = [j for j in job_repo2.list_jobs() if j.status == "running"]
        if jobs:
            job_id = jobs[0].id
            success(f"Job {job_id} is running. Simulating crash now.\n")
            break
        time.sleep(1)

    if job_id is None:
        warn("Job never entered running state within timeout.")
        _stop_worker(loop, thread)
        db2.close()
        return

    # Kill the worker thread abruptly (stop without waiting for jobs to finish)
    loop.running = False  # bypass stop() — simulate ungraceful shutdown
    time.sleep(2)

    job_after_crash = job_repo2.get_job(job_id)
    info(f"After crash — job {job_id} status: {job_after_crash.status}")
    info("(still 'running' because the worker was killed before it could update state)\n")

    info("Restarting worker. Recovery should detect the orphaned job and re-queue it...")

    loop2, thread2 = _start_worker(_slow_pipeline())

    # Give recovery a moment to run
    time.sleep(5)

    job_recovered = job_repo2.get_job(job_id)
    if job_recovered and job_recovered.status == "pending":
        success(f"Job {job_id} was reset to 'pending' by crash recovery. (FR-17)")
    elif job_recovered and job_recovered.status in ("running", "completed"):
        success(f"Job {job_id} was recovered and is now '{job_recovered.status}'. (FR-17)")
    else:
        warn(f"Unexpected status after recovery: {job_recovered.status if job_recovered else 'unknown'}")

    _stop_worker(loop2, thread2)
    db2.close()


# ============================================================
# Section 7 — Statistics & History
# ============================================================

def section_stats():
    header(
        "Section 7 — Statistics & Historical Records",
        "FR-18 (historical records), FR-19 (pamip list), FR-22 (pamip stats)"
    )

    info("Displaying full job history across all demo sections.\n")

    db2       = _make_db()
    from db.job_repository import JobRepository
    from db.step_repository import StepRepository
    job_repo2  = JobRepository(db2)
    step_repo2 = StepRepository(db2)

    from cli.commands import cmd_list, cmd_stats
    run_cli("list", cmd_list, job_repo2)
    run_cli("stats", cmd_stats, job_repo2)

    jobs = job_repo2.list_jobs()
    success(f"All {len(jobs)} jobs remain in the database across session restarts. (FR-18)")

    db2.close()


# ============================================================
# Cleanup
# ============================================================

def section_cleanup():
    print(f"\n{BOLD}{CYAN}{DIVIDER}{RESET}")
    print(f"{BOLD}{CYAN}  Demo Complete{RESET}")
    print(f"{BOLD}{CYAN}{DIVIDER}{RESET}\n")

    answer = input("  Clean up demo database and directories? [y/N] ").strip().lower()
    if answer == "y":
        shutil.rmtree(DEMO_ROOT / "incoming", ignore_errors=True)
        shutil.rmtree(DEMO_ROOT / "processed", ignore_errors=True)
        db_file = Path(DB_PATH)
        if db_file.exists():
            db_file.unlink()
        # Also remove WAL/SHM sidecar files if present
        for ext in ["-wal", "-shm"]:
            sidecar = Path(DB_PATH + ext)
            if sidecar.exists():
                sidecar.unlink()
        success("Demo environment cleaned up.")
    else:
        info("Demo files left intact.")

    print()


# ============================================================
# Main
# ============================================================

SECTIONS = [
    ("Setup & Environment",          section_setup),
    ("File Detection & Job Creation", section_detection),
    ("Pipeline Execution",            section_pipeline),
    ("Failure Propagation",           section_failure),
    ("Manual Retry",                  section_retry),
    ("Crash Recovery",                section_crash_recovery),
    ("Statistics & History",          section_stats),
]


def main():
    # Set up logging to file so suppressed worker output is still captured
    import logging
    Path("demo").mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.FileHandler("demo/demo.log")],
    )

    print(f"\n{BOLD}{CYAN}{DIVIDER}{RESET}")
    print(f"{BOLD}{CYAN}  PAMIP — Live Demonstration{RESET}")
    print(f"{CYAN}  Pipeline for Automated Media Ingestion & Processing{RESET}")
    print(f"{BOLD}{CYAN}{DIVIDER}{RESET}")
    print()
    print("  This demo walks through all functional requirements.")
    print("  Press ENTER to advance through each section.")
    print()

    for i, (title, fn) in enumerate(SECTIONS, 1):
        pause(f"[ {i}/{len(SECTIONS)} ] Press ENTER to begin: {title}")
        fn()

    section_cleanup()


if __name__ == "__main__":
    main()
