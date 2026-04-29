"""
test_commands.py
Tests for the CLI command handlers in cli/commands.py.
Captures stdout via io.StringIO and inspects DB state after each call.

Run from the project root:
    python -m tests.test_commands
"""

import io
import sqlite3
import sys
from contextlib import contextmanager, redirect_stdout


# ----------------------------------------
# In-memory Database stand-in (same shape as test_repositories)
# ----------------------------------------

class InMemoryDatabase:
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


def run_tests():
    from db.job_repository import JobRepository
    from db.step_repository import StepRepository
    from cli.commands import cmd_list, cmd_show, cmd_retry, cmd_stats

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

    def capture(fn, *args, **kwargs):
        """Run fn and return its captured stdout."""
        buf = io.StringIO()
        with redirect_stdout(buf):
            fn(*args, **kwargs)
        return buf.getvalue()

    # ----------------------------------------
    # Helpers
    # ----------------------------------------

    def make_repos():
        db = InMemoryDatabase()
        return db, JobRepository(db), StepRepository(db)

    def seed_failed_job(db, job_repo, step_repo, file_path="/media/x.mp4"):
        """
        Seed a job in the 'failed' state with a mix of step statuses:
        step 1 completed, step 2 failed, step 3 pending. Mirrors the state a
        real failure would leave behind.
        """
        job_id = job_repo.create_job(file_path)
        with db.transaction():
            step_repo.create_steps(job_id, [
                {"step_name": "step1", "max_attempts": 1},
                {"step_name": "step2", "max_attempts": 1},
                {"step_name": "step3", "max_attempts": 1},
            ])
        steps = step_repo.get_steps_for_job(job_id)
        step_repo.update_step_status(steps[0].id, "completed")
        step_repo.update_step_status(steps[1].id, "failed", exit_code=1, stderr="boom")
        # step 3 stays pending
        # Manually drive state into 'failed' bypassing JobManager's guards
        job_repo.update_status(job_id, "running")
        job_repo.update_status(job_id, "failed")
        return job_id

    # ----------------------------------------
    # cmd_list — FR-19
    # ----------------------------------------
    print("\n--- cmd_list (FR-19) ---")

    db, job_repo, step_repo = make_repos()

    out = capture(cmd_list, job_repo)
    check("empty: no-jobs message",          "No jobs found" in out)

    job1 = job_repo.create_job("/media/a.mp4")
    job2 = job_repo.create_job("/media/b.mp4")
    job_repo.update_status(job2, "running")
    job_repo.update_status(job2, "completed")

    out = capture(cmd_list, job_repo)
    check("list: shows job 1",                str(job1) in out)
    check("list: shows job 2",                str(job2) in out)
    check("list: shows file paths",           "/media/a.mp4" in out and "/media/b.mp4" in out)
    check("list: shows status column",        "completed" in out and "pending" in out)  # FR-19

    # ----------------------------------------
    # cmd_show — FR-20
    # ----------------------------------------
    print("\n--- cmd_show (FR-20) ---")

    db, job_repo, step_repo = make_repos()

    out = capture(cmd_show, job_repo, step_repo, 9999)
    check("show: missing id reports not found",  "not found" in out.lower())

    job_id = seed_failed_job(db, job_repo, step_repo)

    out = capture(cmd_show, job_repo, step_repo, job_id)
    check("show: includes job id",               str(job_id) in out)              # FR-20
    check("show: includes file path",            "/media/x.mp4" in out)           # FR-20
    check("show: includes status",               "failed" in out)                 # FR-20
    check("show: lists steps by name",
          "step1" in out and "step2" in out and "step3" in out)                   # FR-20
    # Failed steps have their stderr printed
    check("show: prints failed step stderr",     "boom" in out)                   # FR-20

    # ----------------------------------------
    # cmd_retry — FR-21
    # ----------------------------------------
    print("\n--- cmd_retry (FR-21) ---")

    # Missing job
    db, job_repo, step_repo = make_repos()
    out = capture(cmd_retry, db, job_repo, step_repo, 9999)
    check("retry: missing id reports not found", "not found" in out.lower())

    # Wrong status — pending job can't be retried
    db, job_repo, step_repo = make_repos()
    pending_id = job_repo.create_job("/media/p.mp4")
    out = capture(cmd_retry, db, job_repo, step_repo, pending_id)
    check("retry: refuses non-failed jobs",
          "only failed jobs" in out.lower() or "pending" in out.lower())
    check("retry: pending job status unchanged",
          job_repo.get_job(pending_id).status == "pending")

    # Happy path — failed job is reset and retry counter increments
    db, job_repo, step_repo = make_repos()
    job_id = seed_failed_job(db, job_repo, step_repo)

    before = job_repo.get_job(job_id)
    capture(cmd_retry, db, job_repo, step_repo, job_id)
    after = job_repo.get_job(job_id)

    check("retry: status flipped failed -> pending",  after.status == "pending")    # FR-21
    check("retry: retry_count incremented",           after.retry_count == before.retry_count + 1)  # FR-21

    steps_after = step_repo.get_steps_for_job(job_id)
    check("retry: completed step preserved",          steps_after[0].status == "completed")
    check("retry: failed step reset to pending",      steps_after[1].status == "pending")
    check("retry: pending step still pending",        steps_after[2].status == "pending")

    # Retrying twice in a row increments the counter twice — proves it's the
    # canonical job-level event and not a one-shot trigger.
    job_repo.update_status(job_id, "running")
    job_repo.update_status(job_id, "failed")
    capture(cmd_retry, db, job_repo, step_repo, job_id)
    check("retry: second retry increments again",
          job_repo.get_job(job_id).retry_count == before.retry_count + 2)           # FR-21

    # ----------------------------------------
    # cmd_stats — FR-22
    # ----------------------------------------
    print("\n--- cmd_stats (FR-22) ---")

    db, job_repo, step_repo = make_repos()

    # Empty DB: all four buckets shown as 0
    out = capture(cmd_stats, job_repo)
    check("stats: empty shows pending row",       "pending" in out)
    check("stats: empty shows running row",       "running" in out)
    check("stats: empty shows completed row",     "completed" in out)
    check("stats: empty shows failed row",        "failed" in out)

    # Seed a mix and verify counts appear
    pending_id   = job_repo.create_job("/media/p.mp4")
    completed_id = job_repo.create_job("/media/c.mp4")
    job_repo.update_status(completed_id, "running")
    job_repo.update_status(completed_id, "completed")
    failed_id = job_repo.create_job("/media/f.mp4")
    job_repo.update_status(failed_id, "running")
    job_repo.update_status(failed_id, "failed")
    # Two more pending so pending count is 3
    job_repo.create_job("/media/p2.mp4")
    job_repo.create_job("/media/p3.mp4")

    out = capture(cmd_stats, job_repo)
    # Look for the row "pending" alongside "3"
    lines = out.splitlines()
    pending_line   = next((l for l in lines if "pending"   in l), "")
    completed_line = next((l for l in lines if "completed" in l), "")
    failed_line    = next((l for l in lines if "failed"    in l), "")

    check("stats: pending count = 3",        "3" in pending_line)        # FR-22
    check("stats: completed count = 1",      "1" in completed_line)      # FR-22
    check("stats: failed count = 1",         "1" in failed_line)         # FR-22

    print(f"\nResults: {passed} passed, {failed} failed\n")
    return failed


if __name__ == "__main__":
    failures = run_tests()
    sys.exit(1 if failures else 0)
