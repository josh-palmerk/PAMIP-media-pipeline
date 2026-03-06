"""
test_models.py
Tests for Job and Step dataclasses in jobs/models.py.
Verifies that from_row() correctly maps sqlite3.Row-like data
and that all schema fields are represented.
"""

import sqlite3


# ----------------------------------------
# Helpers — simulate sqlite3.Row objects
# ----------------------------------------

def make_job_row(**overrides) -> sqlite3.Row:
    """Build a sqlite3.Row representing a jobs table record."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            started_at DATETIME,
            finished_at DATETIME,
            retry_count INTEGER NOT NULL DEFAULT 0
        );
    """)

    defaults = {
        "file_path": "/media/video.mp4",
        "status": "pending",
        "started_at": None,
        "finished_at": None,
        "retry_count": 0,
    }
    defaults.update(overrides)

    conn.execute(
        "INSERT INTO jobs (file_path, status, started_at, finished_at, retry_count) VALUES (?, ?, ?, ?, ?);",
        (defaults["file_path"], defaults["status"], defaults["started_at"], defaults["finished_at"], defaults["retry_count"])
    )
    return conn.execute("SELECT * FROM jobs LIMIT 1;").fetchone()


def make_step_row(**overrides) -> sqlite3.Row:
    """Build a sqlite3.Row representing a steps table record."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE steps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            step_name TEXT NOT NULL,
            step_order INTEGER NOT NULL,
            status TEXT NOT NULL,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 1,
            started_at DATETIME,
            finished_at DATETIME,
            exit_code INTEGER,
            stdout TEXT,
            stderr TEXT
        );
    """)

    defaults = {
        "job_id": 1,
        "step_name": "transcode",
        "step_order": 1,
        "status": "pending",
        "attempt_count": 0,
        "max_attempts": 3,
        "started_at": None,
        "finished_at": None,
        "exit_code": None,
        "stdout": None,
        "stderr": None,
    }
    defaults.update(overrides)

    conn.execute(
        """INSERT INTO steps
           (job_id, step_name, step_order, status, attempt_count, max_attempts,
            started_at, finished_at, exit_code, stdout, stderr)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""",
        (defaults["job_id"], defaults["step_name"], defaults["step_order"],
         defaults["status"], defaults["attempt_count"], defaults["max_attempts"],
         defaults["started_at"], defaults["finished_at"], defaults["exit_code"],
         defaults["stdout"], defaults["stderr"])
    )
    return conn.execute("SELECT * FROM steps LIMIT 1;").fetchone()


# ----------------------------------------
# Test runner
# ----------------------------------------

def run_tests():
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

    from jobs.models import Job, Step

    print("\n--- Job Tests ---")

    row = make_job_row()
    job = Job.from_row(row)
    check("id is int",                      isinstance(job.id, int))
    check("file_path matches",              job.file_path == "/media/video.mp4")
    check("status matches",                 job.status == "pending")
    check("retry_count is 0",              job.retry_count == 0)
    check("created_at is set",             job.created_at is not None)
    check("updated_at is set",             job.updated_at is not None)
    check("started_at is None",            job.started_at is None)
    check("finished_at is None",           job.finished_at is None)

    row = make_job_row(status="failed", retry_count=2, started_at="2025-01-01 10:00:00")
    job = Job.from_row(row)
    check("status=failed",                 job.status == "failed")
    check("retry_count=2",                 job.retry_count == 2)
    check("started_at is set",             job.started_at == "2025-01-01 10:00:00")

    print("\n--- Step Tests ---")

    row = make_step_row()
    step = Step.from_row(row)
    check("id is int",                     isinstance(step.id, int))
    check("job_id matches",                step.job_id == 1)
    check("step_name matches",             step.step_name == "transcode")
    check("step_order matches",            step.step_order == 1)
    check("status matches",                step.status == "pending")
    check("attempt_count is 0",           step.attempt_count == 0)
    check("max_attempts is 3",            step.max_attempts == 3)
    check("started_at is None",           step.started_at is None)
    check("finished_at is None",          step.finished_at is None)
    check("exit_code is None",            step.exit_code is None)
    check("stdout is None",               step.stdout is None)
    check("stderr is None",               step.stderr is None)

    row = make_step_row(status="failed", attempt_count=1, exit_code=1, stderr="error msg")
    step = Step.from_row(row)
    check("status=failed",                step.status == "failed")
    check("attempt_count=1",              step.attempt_count == 1)
    check("exit_code=1",                  step.exit_code == 1)
    check("stderr captured",              step.stderr == "error msg")

    print(f"\nResults: {passed} passed, {failed} failed\n")


if __name__ == "__main__":
    run_tests()
