"""
test_repositories.py
Integration tests for JobRepository, StepRepository, and JobManager.
Uses an in-memory SQLite database — no files written to disk.

Run from the project root:
    python test_repositories.py
"""

import sqlite3
import sys
from contextlib import contextmanager


# ----------------------------------------
# Minimal in-memory Database stand-in
# ----------------------------------------

class InMemoryDatabase:
    """
    InMemoryDatabase
    Lightweight substitute for db.Database using SQLite in-memory.
    Replicates the interface used by the repositories and JobManager.
    """

    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self.conn.isolation_level = None
        self._initialize_schema()

    def _initialize_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN (
                    'pending','running','completed','failed'
                )),
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                started_at DATETIME,
                finished_at DATETIME,
                retry_count INTEGER NOT NULL DEFAULT 0
            );
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS steps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL,
                step_name TEXT NOT NULL,
                step_order INTEGER NOT NULL,
                status TEXT NOT NULL CHECK(status IN (
                    'pending','running','completed','failed'
                )),
                attempt_count INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 1,
                started_at DATETIME,
                finished_at DATETIME,
                exit_code INTEGER,
                stdout TEXT,
                stderr TEXT,
                FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
            );
        """)

    def execute(self, sql, params=()):
        return self.conn.execute(sql, params)

    def executemany(self, sql, param_list):
        return self.conn.executemany(sql, param_list)

    def fetchone(self, sql, params=()):
        return self.conn.execute(sql, params).fetchone()

    def fetchall(self, sql, params=()):
        return self.conn.execute(sql, params).fetchall()

    @contextmanager
    def transaction(self):
        try:
            self.conn.execute("BEGIN")
            yield
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def close(self):
        self.conn.close()


# ----------------------------------------
# Test runner
# ----------------------------------------

def run_tests():
    # Import here so path issues surface clearly
    from db.job_repository import JobRepository
    from db.step_repository import StepRepository
    from core.job_manager import JobManager
    from jobs.models import Job, Step

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

    def make_db():
        """Fresh in-memory DB for each test group."""
        return InMemoryDatabase()

    def seed_job(job_repo, file_path="/media/video.mp4") -> int:
        """Insert a pending job and return its ID."""
        return job_repo.create_job(file_path)

    def seed_steps(db, step_repo, job_id, step_defs=None):
        """Insert steps for a job inside a transaction."""
        if step_defs is None:
            step_defs = [
                {"step_name": "step1", "max_attempts": 2},
                {"step_name": "step2", "max_attempts": 2},
            ]
        with db.transaction():
            step_repo.create_steps(job_id, step_defs)

    # ----------------------------------------
    # JobRepository Tests
    # ----------------------------------------
    print("\n--- JobRepository ---")

    db = make_db()
    job_repo = JobRepository(db)

    job_id = seed_job(job_repo)
    check("create_job returns int",             isinstance(job_id, int))
    check("create_job returns positive id",     job_id > 0)

    job = job_repo.get_job(job_id)
    check("get_job returns Job instance",       isinstance(job, Job))
    check("get_job file_path matches",          job.file_path == "/media/video.mp4")
    check("get_job status is pending",          job.status == "pending")
    check("get_job retry_count is 0",           job.retry_count == 0)
    check("get_job created_at is set",          job.created_at is not None)
    check("get_job started_at is None",         job.started_at is None)
    check("get_job finished_at is None",        job.finished_at is None)

    check("get_job missing returns None",       job_repo.get_job(9999) is None)

    next_job = job_repo.get_next_pending_job()
    check("get_next_pending_job returns Job",   isinstance(next_job, Job))
    check("get_next_pending_job correct id",    next_job.id == job_id)

    job_repo.update_status(job_id, "running")
    check("get_next_pending_job none when running",
          job_repo.get_next_pending_job() is None)

    job_repo.update_status(job_id, "completed")
    updated = job_repo.get_job(job_id)
    check("update_status persists",            updated.status == "completed")

    seed_job(job_repo, "/media/b.mp4")
    seed_job(job_repo, "/media/c.mp4")
    jobs = job_repo.list_jobs()
    check("list_jobs returns list",             isinstance(jobs, list))
    check("list_jobs returns all jobs",         len(jobs) == 3)
    check("list_jobs items are Job instances",  all(isinstance(j, Job) for j in jobs))

    job_repo.increment_retry(job_id)
    check("increment_retry increments count",
          job_repo.get_job(job_id).retry_count == 1)
    job_repo.increment_retry(job_id)
    check("increment_retry increments again",
          job_repo.get_job(job_id).retry_count == 2)

    # ----------------------------------------
    # StepRepository Tests
    # ----------------------------------------
    print("\n--- StepRepository ---")

    db = make_db()
    job_repo = JobRepository(db)
    step_repo = StepRepository(db)

    job_id = seed_job(job_repo)
    seed_steps(db, step_repo, job_id)

    steps = step_repo.get_steps_for_job(job_id)
    check("get_steps_for_job returns list",         isinstance(steps, list))
    check("get_steps_for_job correct count",        len(steps) == 2)
    check("get_steps_for_job items are Step",       all(isinstance(s, Step) for s in steps))
    check("get_steps_for_job ordered by step_order",
          steps[0].step_order == 1 and steps[1].step_order == 2)
    check("get_steps_for_job step names correct",
          steps[0].step_name == "step1" and steps[1].step_name == "step2")
    check("get_steps_for_job status is pending",
          all(s.status == "pending" for s in steps))
    check("get_steps_for_job max_attempts correct",
          all(s.max_attempts == 2 for s in steps))

    step = step_repo.get_step(steps[0].id)
    check("get_step returns Step instance",         isinstance(step, Step))
    check("get_step correct id",                    step.id == steps[0].id)
    check("get_step missing returns None",          step_repo.get_step(9999) is None)

    step_repo.update_step_status(step.id, "running")
    updated = step_repo.get_step(step.id)
    check("update_step_status status persists",     updated.status == "running")
    check("update_step_status finished_at set",     updated.finished_at is not None)

    step_repo.update_step_status(
        step.id, "failed",
        exit_code=1,
        stdout="out",
        stderr="err",
        attempt_count=1
    )
    updated = step_repo.get_step(step.id)
    check("update_step_status all fields persist",
          updated.status == "failed"
          and updated.exit_code == 1
          and updated.stdout == "out"
          and updated.stderr == "err"
          and updated.attempt_count == 1)

    # attempt_count unchanged when not provided
    step_repo.update_step_status(step.id, "pending")
    updated = step_repo.get_step(step.id)
    check("update_step_status attempt_count unchanged when omitted",
          updated.attempt_count == 1)

    # steps isolated between jobs
    job_id_2 = seed_job(job_repo, "/media/other.mp4")
    seed_steps(db, step_repo, job_id_2, [{"step_name": "only_step"}])
    check("get_steps_for_job isolates by job_id",
          len(step_repo.get_steps_for_job(job_id_2)) == 1)

    # ----------------------------------------
    # JobManager Tests
    # ----------------------------------------
    print("\n--- JobManager ---")

    def make_manager():
        db = make_db()
        job_repo = JobRepository(db)
        step_repo = StepRepository(db)
        manager = JobManager(db, job_repo, step_repo)
        return db, job_repo, step_repo, manager

    # -- Happy path: all steps succeed --
    db, job_repo, step_repo, manager = make_manager()
    job_id = seed_job(job_repo)
    seed_steps(db, step_repo, job_id)

    def executor_success(step):
        return {"success": True, "exit_code": 0, "stdout": "ok", "stderr": ""}

    manager.process_job(job_id, executor_success)
    job = job_repo.get_job(job_id)
    steps = step_repo.get_steps_for_job(job_id)
    check("happy path: job completed",          job.status == "completed")
    check("happy path: all steps completed",
          all(s.status == "completed" for s in steps))
    check("happy path: stdout captured",
          all(s.stdout == "ok" for s in steps))

    # -- Failure: step fails and exhausts retries --
    db, job_repo, step_repo, manager = make_manager()
    job_id = seed_job(job_repo)
    seed_steps(db, step_repo, job_id, [
        {"step_name": "step1", "max_attempts": 1},
        {"step_name": "step2", "max_attempts": 1},
    ])

    def executor_fail_always(step):
        return {"success": False, "exit_code": 1, "stdout": "", "stderr": "boom"}

    manager.process_job(job_id, executor_fail_always)
    job = job_repo.get_job(job_id)
    steps = step_repo.get_steps_for_job(job_id)
    check("terminal failure: job failed",       job.status == "failed")
    check("terminal failure: step1 failed",     steps[0].status == "failed")
    check("terminal failure: step2 not run",    steps[1].status == "pending")
    check("terminal failure: stderr captured",  steps[0].stderr == "boom")

    # -- Retry: step fails once then succeeds --
    db, job_repo, step_repo, manager = make_manager()
    job_id = seed_job(job_repo)
    seed_steps(db, step_repo, job_id, [
        {"step_name": "step1", "max_attempts": 2},
        {"step_name": "step2", "max_attempts": 2},
    ])

    attempt_tracker = {"step1": 0}

    def executor_retry(step):
        if step.step_name == "step1":
            attempt_tracker["step1"] += 1
            if attempt_tracker["step1"] < 2:
                return {"success": False, "exit_code": 1, "stdout": "", "stderr": "retry me"}
        return {"success": True, "exit_code": 0, "stdout": "ok", "stderr": ""}

    manager.process_job(job_id, executor_retry)
    job = job_repo.get_job(job_id)
    check("retry path: job completed",          job.status == "completed")
    check("retry path: retry_count incremented", job.retry_count == 1)
    check("retry path: step1 attempt_count=1",
          step_repo.get_steps_for_job(job_id)[0].attempt_count == 1)

    # -- Invalid transition raises --
    db, job_repo, step_repo, manager = make_manager()
    job_id = seed_job(job_repo)
    try:
        manager.complete_job(job_id)  # pending -> completed is invalid
        check("invalid transition raises ValueError", False)
    except ValueError:
        check("invalid transition raises ValueError", True)

    # -- Missing job raises --
    db, job_repo, step_repo, manager = make_manager()
    try:
        manager.start_job(9999)
        check("missing job raises ValueError", False)
    except ValueError:
        check("missing job raises ValueError", True)

    print(f"\nResults: {passed} passed, {failed} failed\n")
    return failed


if __name__ == "__main__":
    failures = run_tests()
    sys.exit(1 if failures else 0)
