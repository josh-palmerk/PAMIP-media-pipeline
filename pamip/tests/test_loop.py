"""
test_loop.py
Tests for WorkerLoop. Currently focused on crash recovery (FR-17);
broader loop behavior (file dispatch, concurrency) is exercised via
the running worker rather than unit tests.

Run from the project root:
    python -m tests.test_loop
"""

import sys
import sqlite3
from contextlib import contextmanager


# ----------------------------------------
# Minimal in-memory Database stand-in
# Mirrors the one in test_repositories.py — duplicated here so this
# file can be run independently without cross-module imports.
# ----------------------------------------

class InMemoryDatabase:
    """
    InMemoryDatabase
    Lightweight substitute for db.Database using SQLite in-memory.
    Replicates the interface used by the repositories and WorkerLoop.
    """

    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self.conn.isolation_level = None
        # WorkerLoop reads this on construction to spawn per-thread connections.
        # Recovery tests don't spawn threads, but the constructor still touches it.
        self.db_path = ":memory:"
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
    from db.job_repository import JobRepository
    from db.step_repository import StepRepository
    from worker.loop import WorkerLoop

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

    def make_loop(db, job_repo, step_repo) -> WorkerLoop:
        """
        make_loop
        Constructs a WorkerLoop with stubbed-out collaborators.
        _recover_orphaned_jobs only depends on db, job_repo, and step_repo,
        so engine/watcher/pipeline can be None for these tests.
        """
        return WorkerLoop(
            db=                  db,
            job_repo=            job_repo,
            step_repo=           step_repo,
            engine=              None,
            watcher=             None,
            pipeline=            [],
            output_dir=          "/tmp/unused",
            poll_interval=       1,
            max_concurrent_jobs= 1,
        )

    def seed_running_job(db, job_repo, step_repo, file_path: str,
                         step_states: list[str]) -> int:
        """
        seed_running_job
        Inserts a job in the 'running' state with steps in the given states.
        Returns the new job's id.
        """
        job_id = job_repo.create_job(file_path)
        with db.transaction():
            step_repo.create_steps(job_id, [
                {"step_name": f"step_{i}", "max_attempts": 1}
                for i in range(len(step_states))
            ])
        # Move job to running, bypassing JobManager's transition guard
        # because we're simulating a state left behind by a crash.
        job_repo.update_status(job_id, "running")

        # Apply the requested step states directly via the repo
        steps = step_repo.get_steps_for_job(job_id)
        for step, state in zip(steps, step_states):
            if state != "pending":
                step_repo.update_step_status(step.id, state)
        return job_id

    # ----------------------------------------
    # FR-17: Crash recovery
    # ----------------------------------------
    print("\n--- WorkerLoop crash recovery (FR-17) ---")

    # No orphans — no-op, doesn't raise
    db = InMemoryDatabase()
    job_repo = JobRepository(db)
    step_repo = StepRepository(db)
    loop = make_loop(db, job_repo, step_repo)
    try:
        loop._recover_orphaned_jobs()
        check("no orphans: completes without error", True)
    except Exception as e:
        check(f"no orphans: completes without error (raised {e})", False)

    # Single orphan with a mix of step states — FR-17
    db = InMemoryDatabase()
    job_repo = JobRepository(db)
    step_repo = StepRepository(db)
    loop = make_loop(db, job_repo, step_repo)

    # Job left running with: step 1 completed, step 2 was running mid-crash, step 3 pending
    job_id = seed_running_job(db, job_repo, step_repo, "/media/a.mp4",
                              ["completed", "running", "pending"])

    loop._recover_orphaned_jobs()

    job = job_repo.get_job(job_id)
    steps = step_repo.get_steps_for_job(job_id)
    check("orphan job re-queued to pending",        job.status == "pending")           # FR-17
    check("completed step preserved",               steps[0].status == "completed")    # FR-17
    check("running step reset to pending",          steps[1].status == "pending")      # FR-17
    check("pending step left as pending",           steps[2].status == "pending")      # FR-17

    # Multiple orphans — all re-queued
    db = InMemoryDatabase()
    job_repo = JobRepository(db)
    step_repo = StepRepository(db)
    loop = make_loop(db, job_repo, step_repo)

    orphan_ids = [
        seed_running_job(db, job_repo, step_repo, "/media/a.mp4", ["running"]),
        seed_running_job(db, job_repo, step_repo, "/media/b.mp4", ["running", "pending"]),
        seed_running_job(db, job_repo, step_repo, "/media/c.mp4", ["completed", "running"]),
    ]
    loop._recover_orphaned_jobs()

    statuses = [job_repo.get_job(jid).status for jid in orphan_ids]
    check("multiple orphans: all re-queued to pending",
          all(s == "pending" for s in statuses))                                       # FR-17

    # Non-running jobs are not touched by recovery
    db = InMemoryDatabase()
    job_repo = JobRepository(db)
    step_repo = StepRepository(db)
    loop = make_loop(db, job_repo, step_repo)

    pending_id   = job_repo.create_job("/media/pending.mp4")          # stays pending
    completed_id = job_repo.create_job("/media/completed.mp4")
    job_repo.update_status(completed_id, "running")
    job_repo.update_status(completed_id, "completed")
    failed_id    = job_repo.create_job("/media/failed.mp4")
    job_repo.update_status(failed_id, "running")
    job_repo.update_status(failed_id, "failed")

    loop._recover_orphaned_jobs()

    check("recovery leaves pending jobs alone",
          job_repo.get_job(pending_id).status == "pending")
    check("recovery leaves completed jobs alone",
          job_repo.get_job(completed_id).status == "completed")
    check("recovery leaves failed jobs alone",
          job_repo.get_job(failed_id).status == "failed")

    print(f"\nResults: {passed} passed, {failed} failed\n")
    return failed


if __name__ == "__main__":
    failures = run_tests()
    sys.exit(1 if failures else 0)
