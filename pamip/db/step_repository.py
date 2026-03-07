"""
step_repository.py
Storage layer for step records. All read methods return Step dataclass instances.
Operates on an injected Database connection; does not manage transactions.
"""

from .database import Database
from jobs.models import Step


class StepRepository:
    """
    StepRepository
    Provides CRUD operations for the steps table.
    Accepts a Database instance; callers are responsible for wrapping
    write operations in transactions where atomicity is required.
    """

    def __init__(self, db: Database):
        self.db = db

    def create_steps(self, job_id: int, steps: list[dict]):
        """
        create_steps
        Bulk-inserts a list of step definitions for a given job.
        Step order is assigned automatically by position in the list (1-indexed).

        steps: list of dicts, e.g.:
            [
                {"step_name": "transcode", "max_attempts": 3},
                {"step_name": "thumbnail"},  # max_attempts defaults to 1
            ]
        """
        insert_data = []

        for order, step in enumerate(steps, start=1):
            insert_data.append((
                job_id,
                step["step_name"],
                order,
                "pending",
                0,              # attempt_count
                step.get("max_attempts", 1)
            ))

        self.db.executemany("""
            INSERT INTO steps (
                job_id,
                step_name,
                step_order,
                status,
                attempt_count,
                max_attempts
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, insert_data)

    def get_step(self, step_id: int) -> Step | None:
        """
        get_step
        Fetches a single step by ID.
        Returns a Step dataclass instance, or None if not found.
        """
        cursor = self.db.execute(
            "SELECT * FROM steps WHERE id=?;",
            (step_id,)
        )
        row = cursor.fetchone()
        return Step.from_row(row) if row else None

    def get_steps_for_job(self, job_id: int) -> list[Step]:
        """
        get_steps_for_job
        Fetches all steps for a job, ordered by step_order ascending.
        Returns a list of Step dataclass instances.
        """
        cursor = self.db.execute(
            """
            SELECT * FROM steps
            WHERE job_id=?
            ORDER BY step_order;
            """,
            (job_id,)
        )
        return [Step.from_row(row) for row in cursor.fetchall()]

    def update_step_status(
            self,
            step_id: int,
            status: str,
            exit_code: int | None = None,
            stdout: str | None = None,
            stderr: str | None = None,
            attempt_count: int | None = None
        ):
        """
        update_step_status
        Updates a step's status and optionally its output fields.
        attempt_count is only updated if explicitly provided (via COALESCE).
        finished_at is always set to the current timestamp on update.
        """
        self.db.execute(
            """
            UPDATE steps
            SET status=?,
                exit_code=?,
                stdout=?,
                stderr=?,
                attempt_count=COALESCE(?, attempt_count),
                finished_at=CURRENT_TIMESTAMP
            WHERE id=?;
            """,
            (status, exit_code, stdout, stderr, attempt_count, step_id)
        )
