"""
worker/loop.py
Main worker loop. Polls for new files, creates jobs, and dispatches
them to the pipeline engine. Supports configurable concurrency via
a thread pool. Recovers orphaned 'running' jobs on startup.
"""

import time
import shutil
import threading
import logging
from pathlib import Path

from config import StepConfig
from db.database import Database
from db.job_repository import JobRepository
from db.step_repository import StepRepository
from core.job_manager import JobManager
from ingestion.watcher import FileWatcher
from pipeline.engine import PipelineEngine


log = logging.getLogger(__name__)


class WorkerLoop:
    """
    WorkerLoop
    Drives the main processing loop for PAMIP.

    On startup, re-queues any jobs left in 'running' state from a previous
    crash. Then repeatedly scans for new files, creates jobs, and dispatches
    pending jobs to the pipeline engine up to the configured concurrency limit.

    Each job is executed in its own thread so multiple jobs can run
    simultaneously when max_concurrent_jobs > 1. Each thread opens its own
    database connection to avoid SQLite cross-thread conflicts.

    Usage:
        loop = WorkerLoop(db, job_repo, step_repo, engine, watcher, config)
        loop.start()   # blocks until loop.stop() is called
    """

    def __init__(
        self,
        db:           Database,
        job_repo:     JobRepository,
        step_repo:    StepRepository,
        engine:       PipelineEngine,
        watcher:      FileWatcher,
        pipeline:     list[StepConfig],
        output_dir:   str,
        poll_interval:      int,
        max_concurrent_jobs: int,
    ):
        """
        __init__
        Args:
            db                   (Database)         — shared database connection (main thread only)
            job_repo             (JobRepository)    — job persistence layer
            step_repo            (StepRepository)   — step persistence layer
            engine               (PipelineEngine)   — step executor
            watcher              (FileWatcher)      — file system scanner
            pipeline             (list[StepConfig]) — step definitions used to seed each new job
            output_dir           (str)              — directory to move files after successful processing
            poll_interval        (int)              — seconds between scan iterations
            max_concurrent_jobs  (int)              — maximum simultaneous jobs
        """
        self.db                  = db
        self.db_path             = str(db.db_path)  # stored so job threads can open their own connection
        self.job_repo            = job_repo
        self.step_repo           = step_repo
        self.engine              = engine
        self.watcher             = watcher
        self.pipeline            = pipeline
        self.output_dir          = Path(output_dir)
        self.poll_interval       = poll_interval
        self.max_concurrent_jobs = max_concurrent_jobs

        self.running = False

        # Tracks active job threads: job_id -> Thread
        self._active: dict[int, threading.Thread] = {}
        self._lock = threading.Lock()  # guards _active

    # ----------------------------------------
    # Startup / Shutdown
    # ----------------------------------------

    def start(self):
        """
        start
        Recovers orphaned jobs then enters the main polling loop.
        Blocks until stop() is called.
        """
        log.info("Worker starting.")
        print("Worker starting.")

        self._recover_orphaned_jobs()

        self.running = True
        while self.running:
            try:
                self._iteration()
            except Exception as e:
                log.error(f"Unhandled error in worker loop: {e}", exc_info=True)
                print(f"[worker] Error: {e}")

            time.sleep(self.poll_interval)

        log.info("Worker stopped.")
        print("Worker stopped.")

    def stop(self):
        """
        stop
        Signals the loop to exit after the current iteration completes.
        Does not forcibly terminate any running job threads.
        """
        self.running = False

    # ----------------------------------------
    # Crash Recovery — FR-17
    # ----------------------------------------

    def _recover_orphaned_jobs(self):
        """
        _recover_orphaned_jobs
        Re-queues any jobs stuck in 'running' state from a previous crash.
        Resets them to 'pending' so they will be picked up on the next iteration.
        """
        orphans = [j for j in self.job_repo.list_jobs() if j.status == "running"]

        if not orphans:
            return

        log.warning(f"Found {len(orphans)} orphaned job(s) from previous session. Re-queuing.")
        print(f"[worker] Recovering {len(orphans)} orphaned job(s).")

        for job in orphans:
            with self.db.transaction():
                self.job_repo.update_status(job.id, "failed")
                # Reset all non-completed steps to pending so the job
                # can be retried cleanly from the beginning
                for step in self.step_repo.get_steps_for_job(job.id):
                    if step.status != "completed":
                        self.step_repo.update_step_status(step.id, "pending")
                self.job_repo.update_status(job.id, "pending")

    # ----------------------------------------
    # Main Iteration
    # ----------------------------------------

    def _iteration(self):
        """
        _iteration
        Single pass of the worker loop:
            1. Scan for new files and create jobs with pipeline steps
            2. Clean up finished threads
            3. Dispatch pending jobs up to the concurrency limit
        """
        # 1. Detect new files and create a job for each — FR-1, FR-4
        new_files = self.watcher.scan()
        for file_path in new_files:
            with self.db.transaction():
                job_id = self.job_repo.create_job(str(file_path))
                # Seed pipeline steps from config — FR-12
                self.step_repo.create_steps(job_id, [
                    {"step_name": s.step_name, "max_attempts": s.max_attempts}
                    for s in self.pipeline
                ])
            log.info(f"Created job {job_id} for file: {file_path}")
            print(f"[worker] New file detected: {file_path} -> job {job_id}")

        # 2. Remove references to threads that have finished
        with self._lock:
            finished = [jid for jid, t in self._active.items() if not t.is_alive()]
            for jid in finished:
                del self._active[jid]

        # 3. Dispatch pending jobs up to the concurrency limit
        with self._lock:
            slots_available = self.max_concurrent_jobs - len(self._active)

        for _ in range(slots_available):
            job = self.job_repo.get_next_pending_job()
            if not job:
                break  # no more pending jobs

            with self._lock:
                # Guard against dispatching the same job twice if two
                # iterations overlap (shouldn't happen, but defensive)
                if job.id in self._active:
                    continue

                thread = threading.Thread(
                    target=self._run_job,
                    args=(job.id,),
                    daemon=True,
                    name=f"job-{job.id}"
                )
                self._active[job.id] = thread

            thread.start()
            log.info(f"Dispatched job {job.id} to thread {thread.name}")
            print(f"[worker] Dispatched job {job.id}.")

    # ----------------------------------------
    # Job Execution
    # ----------------------------------------

    def _run_job(self, job_id: int):
        """
        _run_job
        Runs a single job in a worker thread.
        Opens a dedicated database connection for this thread — SQLite
        connections cannot be shared across threads.
        On success, moves the source file to the output directory.

        Args:
            job_id (int) — ID of the job to process
        """
        # Each thread gets its own DB connection and repositories
        db        = Database(self.db_path)
        job_repo  = JobRepository(db)
        step_repo = StepRepository(db)
        manager   = JobManager(db, job_repo, step_repo)

        try:
            log.info(f"Job {job_id} started.")
            print(f"[worker] Job {job_id} started.")

            manager.process_job(job_id, self.engine.execute_step)

            # Move source file to output directory on success — FR-2
            job = job_repo.get_job(job_id)
            if job and job.status == "completed":
                src = Path(job.file_path)
                dst = self.output_dir / src.name
                self.output_dir.mkdir(parents=True, exist_ok=True)
                shutil.move(str(src), str(dst))
                log.info(f"Job {job_id} completed. File moved to {dst}")
                print(f"[worker] Job {job_id} completed. File moved to {dst}")

        except Exception as e:
            log.error(f"Job {job_id} failed with exception: {e}", exc_info=True)
            print(f"[worker] Job {job_id} failed: {e}")

        finally:
            db.close()
