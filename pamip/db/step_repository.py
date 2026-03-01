from .database import Database


class StepRepository:
    def __init__(self, db: Database):
        self.db = db

    def create_steps(self, job_id: int, steps: list[dict]):
        """
        steps: list of dicts like:
        [
            {"step_name": "transcode", "max_attempts": 3},
            {"step_name": "thumbnail"},  # defaults max_attempts=1
        ]
        """

        insert_data = []

        for order, step in enumerate(steps, start=1):
            insert_data.append((
                job_id,
                step["step_name"],
                order,
                "pending",
                0,  # attempt_count
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

    def get_step(self, step_id: int):
        cursor = self.db.execute(
            """
            SELECT * FROM steps
            WHERE id=?;
            """,
            (step_id,)
        )
        return cursor.fetchone()

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

    def update_step_status(
            self,
            step_id: int,
            status: str,
            exit_code=None,
            stdout=None,
            stderr=None,
            attempt_count=None
        ):
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
