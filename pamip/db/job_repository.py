"""
job_repository.py
Storage layer for job records. All read methods return Job dataclass instances.
Operates on an injected Database connection; does not manage transactions.
"""

from .database import Database
from jobs.models import Job


class JobRepository:
    """
    JobRepository
    Provides CRUD operations for the jobs table.
    Accepts a Database instance; callers are responsible for wrapping
    write operations in transactions where atomicity is required.
    """

    def __init__(self, db: Database):
        self.db = db

    def create_job(self, file_path: str) -> int:
        """
        create_job
        Inserts a new job record with status 'pending'.
        Returns the new job's integer ID.
        """
        cursor = self.db.execute(
            "INSERT INTO jobs (file_path, status) VALUES (?, 'pending');",
            (file_path,)
        )
        return cursor.lastrowid

    def get_job(self, job_id: int) -> Job | None:
        """
        get_job
        Fetches a single job by ID.
        Returns a Job dataclass instance, or None if not found.
        """
        cursor = self.db.execute(
            "SELECT * FROM jobs WHERE id=?;",
            (job_id,)
        )
        row = cursor.fetchone()
        return Job.from_row(row) if row else None

    def get_next_pending_job(self) -> Job | None:
        """
        get_next_pending_job
        Fetches the oldest pending job by created_at.
        Returns a Job dataclass instance, or None if no pending jobs exist.
        """
        cursor = self.db.execute(
            "SELECT * FROM jobs WHERE status='pending' ORDER BY created_at LIMIT 1;"
        )
        row = cursor.fetchone()
        return Job.from_row(row) if row else None

    def list_jobs(self) -> list[Job]:
        """
        list_jobs
        Fetches all jobs ordered by created_at descending.
        Returns a list of Job dataclass instances.
        """
        cursor = self.db.execute(
            "SELECT * FROM jobs ORDER BY created_at DESC;"
        )
        return [Job.from_row(row) for row in cursor.fetchall()]

    def update_status(self, job_id: int, status: str):
        """
        update_status
        Updates the status and updated_at timestamp for a job.
        """
        self.db.execute(
            """
            UPDATE jobs
            SET status=?, updated_at=CURRENT_TIMESTAMP
            WHERE id=?;
            """,
            (status, job_id)
        )

    def increment_retry(self, job_id: int):
        """
        increment_retry
        Increments the job's retry_count by 1 and updates updated_at.
        """
        self.db.execute(
            """
            UPDATE jobs
            SET retry_count = retry_count + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE id=?;
            """,
            (job_id,)
        )
