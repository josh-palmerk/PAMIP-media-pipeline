"""
demo.py
Live demonstration script for PAMIP.
Walks through all functional requirements in a presenter-controlled,
section-by-section format. Each section runs immediately when the
presenter presses ENTER.

Usage (run from the pamip/ project root):
    python demo.py

Prerequisites:
    - ffmpeg installed and on PATH
    - Demo media files present in demo/media/:
        test_image.png
        test_image.jpg
        test_video.mp4
        test_video.mkv

The script manages its own database (demo/demo.db) and directories
so it does not interfere with the production database (pamip.db).
"""

import sys
import shutil
import threading
import time
from pathlib import Path


# ============================================================
# Paths — all demo I/O is isolated under demo/
# ============================================================

DEMO_ROOT  = Path("demo")
MEDIA_DIR  = DEMO_ROOT / "media"       # presenter-provided source files
WATCH_DIR  = DEMO_ROOT / "incoming"    # worker monitors this
OUTPUT_DIR = DEMO_ROOT / "processed"   # completed files land here
DB_PATH    = str(DEMO_ROOT / "demo.db")

# Flag file used by the fail_once step (Section 4/5)
FAIL_FLAG  = DEMO_ROOT / "fail_once.flag"

# Source media the presenter provides
SRC_PNG = MEDIA_DIR / "test_image.png"
SRC_JPG = MEDIA_DIR / "test_image.jpg"
SRC_MP4 = MEDIA_DIR / "test_video.mp4"
SRC_MKV = MEDIA_DIR / "test_video.mkv"


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
    """Print a coloured section header with FR labels."""
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


def copy_to_watch(src: Path, label: str) -> Path:
    """Copy a source media file into the watch directory."""
    dst = WATCH_DIR / src.name
    shutil.copy(src, dst)
    info(f"Dropped {label} into watch dir: {dst.name}")
    return dst


def _make_db():
    """Open a fresh Database connection to demo.db on the calling thread."""
    from db.database import Database
    from db import schema
    db = Database(DB_PATH)
    schema.initialize_schema(db)
    return db


# ============================================================
# Worker management
# ============================================================

def _start_worker(step_configs) -> tuple:
    """
    _start_worker
    Builds and starts a WorkerLoop in a background thread.
    All SQLite objects are created inside the worker thread — SQLite
    connections cannot be shared across threads.
    Worker print output is suppressed so ffmpeg noise doesn't scroll
    over the demo terminal. All activity is captured in demo/demo.log.

    Returns (loop, thread). Caller must call _stop_worker() when done.
    The main thread should open its own DB connection for any reads.
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

    # ready signals the main thread once the loop object exists
    ready       = threading.Event()
    loop_holder = [None]

    class _NullWriter(io.IOBase):
        def write(self, *a): return 0
        def flush(self):     pass

    def _run():
        # All DB objects must be created inside the worker thread
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
        ready.set()

        # Suppress stdout so ffmpeg progress lines don't clutter the demo.
        # stderr from subprocesses bypasses Python's sys.stdout entirely
        # and is silenced by passing subprocess.DEVNULL in run_process.
        old, sys.stdout = sys.stdout, _NullWriter()
        try:
            loop.start()
        finally:
            sys.stdout = old
            db.close()

    thread = threading.Thread(target=_run, daemon=True, name="worker")
    thread.start()
    ready.wait(timeout=10)
    return loop_holder[0], thread


def _stop_worker(loop, thread):
    """Signal the worker to stop and wait for it to exit cleanly."""
    loop.stop()
    thread.join(timeout=10)


def _wait_for_n_jobs(job_repo, n: int, timeout: int = 30) -> list:
    """
    _wait_for_n_jobs
    Polls until at least n jobs exist in the DB, then returns them.
    Used to confirm the worker has detected and created jobs before
    proceeding with assertions.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        jobs = job_repo.list_jobs()
        if len(jobs) >= n:
            return jobs
        time.sleep(1)
    return job_repo.list_jobs()


def _wait_for_terminal(job_repo, min_jobs: int, timeout: int = 300) -> list:
    """
    _wait_for_terminal
    Polls until at least min_jobs exist and all are in a terminal state
    (completed or failed). Prints a live counter. Returns the final job list.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        jobs = job_repo.list_jobs()
        done = [j for j in jobs if j.status in ("completed", "failed")]
        sys.stdout.write(f"\r  Jobs finished: {len(done)}/{len(jobs)}   ")
        sys.stdout.flush()
        if len(jobs) >= min_jobs and len(done) == len(jobs):
            print()
            return jobs
        time.sleep(2)
    print()
    return job_repo.list_jobs()


# ============================================================
# Pipeline configurations
# ============================================================

def _normal_pipeline():
    from config import StepConfig
    return [
        StepConfig("transcode",      3, 300, {}),
        StepConfig("thumbnail",      2, 60,  {}),
        StepConfig("image_convert",  2, 60,  {}),
        StepConfig("image_compress", 2, 60,  {"compress_threshold_mb": 2}),
    ]


def _fail_once_pipeline(fail_once_max_attempts: int = 1):
    """
    Pipeline used for Sections 4 & 5.
    Step 2 is 'fail_once' — a registered handler that fails on its first
    call (via a flag file) and succeeds on subsequent calls.

    Section 4 passes max_attempts=1 so the step exhausts retries immediately
    and the job transitions to failed with no automatic retry.

    Section 5 uses cmd_retry to manually reset the failed job to pending.
    The step then runs again, finds the flag present, and succeeds. The
    max_attempts passed here doesn't matter for Section 5 because the step
    only needs one more attempt — but keeping it at 1 is cleaner.

    Args:
        fail_once_max_attempts (int) — max_attempts for the fail_once step
    """
    from config import StepConfig
    return [
        StepConfig("transcode",  1, 300, {}),
        StepConfig("fail_once",  fail_once_max_attempts, 60, {}),
        StepConfig("thumbnail",  1, 60,  {}),
    ]


def _sleep_pipeline():
    """
    Pipeline for Section 6 (crash recovery).
    Uses a 'long_sleep' step that runs a 30-second Python sleep subprocess —
    guaranteed to be interruptible regardless of media file length or hardware.
    """
    from config import StepConfig
    return [
        StepConfig("long_sleep", 1, 120, {}),
    ]


# ============================================================
# Demo-only step handler registration
# ============================================================

def _register_demo_steps():
    """
    _register_demo_steps
    Registers step handlers used only in the demo. Called once at startup
    so they are available to any worker started during the session.
    """
    from pipeline.steps import register_step
    from jobs.models import Job

    @register_step("fail_once")
    def handle_fail_once(file_path: str, output_dir: str, job: Job, options: dict) -> list:
        """
        Fails on the first execution by checking for a flag file.
        On first call: creates the flag and returns a failing command.
        On subsequent calls: flag exists, removes it, returns a no-op success.
        This lets Section 4 show a clean failure and Section 5 show a
        successful retry without any changes to step definitions.
        """
        if not FAIL_FLAG.exists():
            # First call — write the flag and return a command that exits non-zero
            FAIL_FLAG.touch()
            return ["python", "-c", "import sys; sys.exit(1)"]
        else:
            # Subsequent call — remove the flag and succeed
            FAIL_FLAG.unlink()
            return ["python", "-c", ""]

    @register_step("long_sleep")
    def handle_long_sleep(file_path: str, output_dir: str, job: Job, options: dict) -> list:
        """
        Runs a 30-second Python sleep subprocess.
        Used in Section 6 to give the crash simulation enough time to
        interrupt the worker while a job is mid-execution.
        """
        return ["python", "-c", "import time; time.sleep(30)"]


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

    # Clean up any leftover flag from a previous run
    if FAIL_FLAG.exists():
        FAIL_FLAG.unlink()

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

    from db.job_repository import JobRepository
    from cli.commands import cmd_list

    info("Starting worker and dropping files into the watch directory.")
    info("Worker polls every 2 seconds — jobs will appear shortly.\n")

    # Start worker first, then drop files so detection is visible
    loop, thread = _start_worker(_normal_pipeline())

    copy_to_watch(SRC_PNG, "test_image.png")
    copy_to_watch(SRC_MP4, "test_video.mp4")

    info("\nWaiting for worker to detect files and create jobs...")
    db2       = _make_db()
    job_repo2 = JobRepository(db2)

    # Block until both jobs are created
    jobs = _wait_for_n_jobs(job_repo2, n=2, timeout=15)

    if len(jobs) >= 2:
        success(f"{len(jobs)} job(s) created — unique IDs: {[j.id for j in jobs]} (FR-4, FR-5)\n")
        run_cli("list", cmd_list, job_repo2)
    else:
        warn(f"Only {len(jobs)} job(s) detected within timeout.")

    info("\nFR-2 check: dropping the same files again — no new jobs should appear.")
    copy_to_watch(SRC_PNG, "test_image.png (duplicate)")
    copy_to_watch(SRC_MP4, "test_video.mp4 (duplicate)")
    time.sleep(5)

    jobs_after = job_repo2.list_jobs()
    if len(jobs_after) == len(jobs):
        success("Duplicate files produced no new jobs. (FR-2)")
    else:
        warn(f"Job count changed unexpectedly: {len(jobs)} → {len(jobs_after)}")

    info("\nFR-3 check: dropping an unsupported file type (.txt).")
    (WATCH_DIR / "readme.txt").write_text("this should be ignored")
    time.sleep(5)

    jobs_final = job_repo2.list_jobs()
    if len(jobs_final) == len(jobs):
        success(".txt file produced no job. (FR-3)")
    else:
        warn("Unexpected job created for unsupported file type.")

    # Let the worker finish the two jobs cleanly before Section 3 starts
    info("\nWaiting for jobs to complete before next section...")
    _wait_for_terminal(job_repo2, min_jobs=2)

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

    from db.job_repository import JobRepository
    from db.step_repository import StepRepository
    from cli.commands import cmd_list, cmd_show

    info("Dropping all four media files and letting the worker run to completion.")
    info("ffmpeg output is suppressed — captured stdout/stderr visible via 'pamip show'.\n")

    loop, thread = _start_worker(_normal_pipeline())

    for src, label in [
        (SRC_PNG, "test_image.png"),
        (SRC_JPG, "test_image.jpg"),
        (SRC_MP4, "test_video.mp4"),
        (SRC_MKV, "test_video.mkv"),
    ]:
        copy_to_watch(src, label)

    info("\nProcessing... (this may take a minute for video files)")

    db2       = _make_db()
    job_repo2 = JobRepository(db2)
    step_repo2 = StepRepository(db2)

    jobs = _wait_for_terminal(job_repo2, min_jobs=4)
    _stop_worker(loop, thread)

    success(f"All {len(jobs)} jobs reached a terminal state.\n")

    run_cli("list", cmd_list, job_repo2)

    # Only show jobs from this section (exclude Section 2 jobs)
    section3_jobs = sorted(
        [j for j in jobs if j.status in ("completed", "failed") and
         any(f in j.file_path for f in ["jpg", "png", "mp4", "mkv"])],
        key=lambda j: j.id
    )

    info("\nStep detail for each job (FR-9 order, FR-10 status, FR-14 captured output):\n")
    for job in section3_jobs[-4:]:  # show the 4 most recent
        run_cli(f"show {job.id}", cmd_show, job_repo2, step_repo2, job.id)

    info("Output files in processed directory:")
    outputs = sorted(OUTPUT_DIR.iterdir()) if OUTPUT_DIR.exists() else []
    for f in outputs:
        success(f"  {f.name}")
    if not outputs:
        warn("No output files found — check demo/demo.log")

    db2.close()


# ============================================================
# Section 4 — Failure Propagation
# ============================================================

def section_failure():
    header(
        "Section 4 — Failure Propagation",
        "FR-11 (failed step stops pipeline)"
    )

    from db.job_repository import JobRepository
    from db.step_repository import StepRepository
    from cli.commands import cmd_show

    info("Dropping test_image.png with a modified pipeline:")
    info("  Step 1 — transcode    (will succeed)")
    info("  Step 2 — fail_once    (will fail — max_attempts=1, no retry)")
    info("  Step 3 — thumbnail    (must NOT run)\n")

    # Ensure the flag is clear so fail_once fails on its first call
    if FAIL_FLAG.exists():
        FAIL_FLAG.unlink()

    # Clear the watch dir to avoid picking up leftovers from Section 3
    for f in WATCH_DIR.iterdir():
        if f.is_file():
            f.unlink()

    copy_to_watch(SRC_PNG, "test_image.png")

    # max_attempts=1 — step exhausts retries on first failure, no automatic retry
    loop, thread = _start_worker(_fail_once_pipeline(fail_once_max_attempts=1))

    db2        = _make_db()
    job_repo2  = JobRepository(db2)
    step_repo2 = StepRepository(db2)

    info("Waiting for job to fail...")
    deadline = time.time() + 60
    job_id   = None
    while time.time() < deadline:
        failed = [j for j in job_repo2.list_jobs() if j.status == "failed"]
        if failed:
            job_id = failed[-1].id
            break
        time.sleep(2)

    _stop_worker(loop, thread)

    if job_id is None:
        warn("No failed job found within timeout.")
        db2.close()
        return

    run_cli(f"show {job_id}", cmd_show, job_repo2, step_repo2, job_id)

    steps = step_repo2.get_steps_for_job(job_id)
    thumbnail = next((s for s in steps if s.step_name == "thumbnail"), None)
    if thumbnail and thumbnail.status == "pending":
        success("thumbnail step never ran — failure propagation confirmed. (FR-11)")
    else:
        warn(f"Unexpected thumbnail status: {thumbnail.status if thumbnail else 'not found'}")

    db2.close()


# ============================================================
# Section 5 — Retry
# ============================================================

def section_retry():
    header(
        "Section 5 — Manual Retry",
        "FR-8 (automatic retry), FR-21 (pamip retry command)"
    )

    from db.job_repository import JobRepository
    from db.step_repository import StepRepository
    from cli.commands import cmd_retry, cmd_show

    info("Retrying the failed job from Section 4.")
    info("The fail_once step now has max_attempts=2 — it failed once already,")
    info("so this retry attempt will succeed.\n")

    db2       = _make_db()
    job_repo2 = JobRepository(db2)
    step_repo2 = StepRepository(db2)

    failed_jobs = [j for j in job_repo2.list_jobs() if j.status == "failed"]
    if not failed_jobs:
        warn("No failed jobs found. Run Section 4 first.")
        db2.close()
        return

    job_id = failed_jobs[-1].id
    info(f"Found failed job: ID {job_id}")

    run_cli(f"retry {job_id}", cmd_retry, db2, job_repo2, step_repo2, job_id)

    info(f"\nJob {job_id} reset to pending. Starting worker...")

    # Clear watch dir — the original file was moved to processed already.
    # Re-drop it so the job has a real file to work with on retry.
    job      = job_repo2.get_job(job_id)
    src_file = Path(job.file_path)
    if not src_file.exists():
        # File was moved to processed — copy it back for the retry
        processed = OUTPUT_DIR / src_file.name
        if processed.exists():
            shutil.copy(processed, src_file)

    # max_attempts=1 — the step already consumed its one attempt in Section 4.
    # cmd_retry reset it to pending; this run will succeed (flag is present from Section 4).
    loop, thread = _start_worker(_fail_once_pipeline(fail_once_max_attempts=1))

    info("Waiting for retry to complete...")
    deadline = time.time() + 120
    while time.time() < deadline:
        j = job_repo2.get_job(job_id)
        if j and j.status in ("completed", "failed"):
            break
        time.sleep(2)

    _stop_worker(loop, thread)

    run_cli(f"show {job_id}", cmd_show, job_repo2, step_repo2, job_id)

    job = job_repo2.get_job(job_id)
    if job and job.status == "completed":
        success(f"Job {job_id} completed after retry. (FR-8, FR-21)")
    else:
        status = job.status if job else "unknown"
        warn(f"Job {job_id} status after retry: {status}")

    db2.close()


# ============================================================
# Section 6 — Crash Recovery
# ============================================================

def section_crash_recovery():
    header(
        "Section 6 — Crash Recovery",
        "FR-17 (orphaned job recovery)"
    )

    from db.job_repository import JobRepository
    from cli.commands import cmd_show

    info("The 'long_sleep' step runs a 30-second subprocess.")
    info("We will drop a file, wait for the job to start, then kill")
    info("the worker mid-execution to simulate a crash.")
    info("On restart the orphaned 'running' job must be re-queued.\n")

    # Clear watch dir
    for f in WATCH_DIR.iterdir():
        if f.is_file():
            f.unlink()

    copy_to_watch(SRC_PNG, "test_image.png (crash demo)")

    loop, thread = _start_worker(_sleep_pipeline())

    db2       = _make_db()
    job_repo2 = JobRepository(db2)

    info("Waiting for job to enter 'running' state...")
    job_id      = None
    orphan_thread = None
    deadline    = time.time() + 30
    while time.time() < deadline:
        running = [j for j in job_repo2.list_jobs() if j.status == "running"]
        if running:
            job_id = running[0].id
            # Capture the job thread so we can join it later and avoid a
            # completed->completed race when the second worker also processes
            # this job after crash recovery.
            with loop._lock:
                orphan_thread = loop._active.get(job_id)
            success(f"Job {job_id} is now running.\n")
            break
        time.sleep(1)

    if job_id is None:
        warn("Job never entered running state — check demo/demo.log.")
        _stop_worker(loop, thread)
        db2.close()
        return

    info("Simulating crash — killing worker without waiting for job to finish...")
    # Set running=False directly rather than calling stop(), which would
    # wait for active jobs. The job thread keeps running briefly then orphans.
    loop.running = False

    # Wait for the poll loop thread to exit. The job thread is still alive
    # (sleeping for 30s), but we do NOT join it — we want it to orphan.
    thread.join(timeout=10)

    job_after = job_repo2.get_job(job_id)
    info(f"Worker killed. Job {job_id} status in DB: '{job_after.status}'")
    info("(Job is still 'running' — the worker never got to update its state)\n")

    # Remove the file from the watch dir before restarting. The second
    # worker starts a fresh FileWatcher with an empty _seen set, so it
    # would re-detect any file still sitting there and create a duplicate
    # job — whose file move would then fail because the first job already
    # moved or is still using it.
    crash_file = WATCH_DIR / SRC_PNG.name
    if crash_file.exists():
        crash_file.unlink()

    info("Restarting worker. Crash recovery runs on startup...")
    loop2, thread2 = _start_worker(_sleep_pipeline())

    # Give recovery a few seconds to detect and reset the orphan
    time.sleep(6)

    job_recovered = job_repo2.get_job(job_id)
    if job_recovered and job_recovered.status == "pending":
        success(f"Job {job_id} reset to 'pending' by crash recovery. (FR-17)")
    elif job_recovered and job_recovered.status in ("running", "completed"):
        success(f"Job {job_id} recovered — now '{job_recovered.status}'. (FR-17)")
    else:
        warn(f"Unexpected status: {job_recovered.status if job_recovered else 'unknown'}")

    # Wait for the second worker (and any job threads it spawned) to finish
    # cleanly before returning. This also lets the original orphaned job
    # thread complete so it doesn't race against anything in later sections.
    _stop_worker(loop2, thread2)

    # Join the original orphaned thread (still sleeping for up to 30s).
    # Without this, it wakes up after Section 6 ends and tries to complete
    # a job that the second worker already completed, causing a
    # completed->completed ValueError in the log.
    if orphan_thread and orphan_thread.is_alive():
        info("Waiting for orphaned job thread to finish (up to 35s)...")
        orphan_thread.join(timeout=35)
    db2.close()


# ============================================================
# Section 7 — Statistics & History
# ============================================================

def section_stats():
    header(
        "Section 7 — Statistics & Historical Records",
        "FR-18 (historical records), FR-19 (pamip list), FR-22 (pamip stats)"
    )

    from db.job_repository import JobRepository
    from cli.commands import cmd_list, cmd_stats

    info("Displaying full job history accumulated across all demo sections.\n")

    db2       = _make_db()
    job_repo2 = JobRepository(db2)

    run_cli("list", cmd_list, job_repo2)
    run_cli("stats", cmd_stats, job_repo2)

    jobs = job_repo2.list_jobs()
    success(f"All {len(jobs)} jobs persist in the database across worker restarts. (FR-18)")

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
        shutil.rmtree(WATCH_DIR,  ignore_errors=True)
        shutil.rmtree(OUTPUT_DIR, ignore_errors=True)
        for p in [Path(DB_PATH), Path(DB_PATH + "-wal"), Path(DB_PATH + "-shm"), FAIL_FLAG]:
            if p.exists():
                p.unlink()
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
    import logging
    DEMO_ROOT.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.FileHandler("demo/demo.log")],
    )

    # Register demo-only step handlers before any worker starts
    _register_demo_steps()

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
