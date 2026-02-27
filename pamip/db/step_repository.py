from .database import Database


class StepRepository:
    def __init__(self, db: Database):
        self.db = db

    def create_steps(self, job_id: int, steps: list[str]):
        self.db.begin()
        try:
            for order, step_name in enumerate(steps, start=1):
                self.db.conn.execute(
                    """
                    INSERT INTO steps (job_id, step_name, step_order, status)
                    VALUES (?, ?, ?, 'pending');
                    """,
                    (job_id, step_name, order)
                )
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

    def get_steps_for_job(self, job_id: int):
        cursor = self.db.execute(
            """
            SELECT * FROM steps
            WHERE job_id=?
            ORDER BY step_order;
            """,
            (job_id,)
        )
        return cursor.fetchall()

    def update_step_status(self, step_id: int, status: str, exit_code=None, stdout=None, stderr=None):
        self.db.execute(
            """
            UPDATE steps
            SET status=?,
                exit_code=?,
                stdout=?,
                stderr=?,
                finished_at=CURRENT_TIMESTAMP
            WHERE id=?;
            """,
            (status, exit_code, stdout, stderr, step_id)
        )
