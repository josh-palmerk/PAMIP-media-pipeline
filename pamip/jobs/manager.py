class JobManager:
    def __init__(self, job_repository):
        self.job_repository = job_repository

    def create_job(self, filename):
        return self.job_repository.create(filename)

    def get_next_pending_job(self):
        return self.job_repository.find_next_pending()

    def mark_running(self, job):
        self._transition(job, "RUNNING")

    def mark_completed(self, job):
        self._transition(job, "COMPLETED")

    def mark_failed(self, job, error_message=None):
        self._transition(job, "FAILED", error_message)

    def retry_job(self, job_id):
        job = self.job_repository.find_by_id(job_id)
        if job.status != "FAILED":
            raise ValueError("Only failed jobs can be retried")
        self._transition(job, "RETRYING")

    def _transition(self, job, new_status, error_message=None):
        if not self._is_valid_transition(job.status, new_status):
            raise ValueError(f"Invalid transition {job.status} → {new_status}")

        self.job_repository.update_status(
            job.id,
            new_status,
            error_message
        )

    def _is_valid_transition(self, old, new):
        allowed = {
            "PENDING": ["RUNNING"],
            "RUNNING": ["COMPLETED", "FAILED"],
            "FAILED": ["RETRYING"],
            "RETRYING": ["RUNNING"],
        }
        return new in allowed.get(old, [])