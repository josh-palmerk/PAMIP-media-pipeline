from .database import Database


class JobRepository:
    def __init__(self, db: Database):
        self.db = db

    def create_job(self, file_path: str) -> int:
        cursor = self.db.execute(
            "INSERT INTO jobs (file_path, status) VALUES (?, 'pending');",
            (file_path,)
        )
        return cursor.lastrowid

    def get_next_pending_job(self):
        cursor = self.db.execute(
            "SELECT * FROM jobs WHERE status='pending' ORDER BY created_at LIMIT 1;"
        )
        return cursor.fetchone()

    def update_status(self, job_id: int, status: str):
        self.db.execute(
            """
            UPDATE jobs
            SET status=?, updated_at=CURRENT_TIMESTAMP
            WHERE id=?;
            """,
            (status, job_id)
        )

    def increment_retry(self, job_id: int):
        self.db.execute(
            """
            UPDATE jobs
            SET retry_count = retry_count + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE id=?;
            """,
            (job_id,)
        )

    def get_job(self, job_id: int):
        cursor = self.db.execute(
            "SELECT * FROM jobs WHERE id=?;",
            (job_id,)
        )
        return cursor.fetchone()

    def list_jobs(self):
        cursor = self.db.execute(
            "SELECT * FROM jobs ORDER BY created_at DESC;"
        )
        return cursor.fetchall()