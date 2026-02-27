class JobManager:
    VALID_TRANSITIONS = {
        "pending": ["running"],
        "running": ["completed", "failed"],
        "failed": ["pending"],     # retry
        "completed": []
    }

    def __init__(self, job_repo, step_repo):
        self.job_repo = job_repo
        self.step_repo = step_repo
        self.pipeline = ["step1", "step2"]

    # ----------------------------------------
    # Transition Core
    # ----------------------------------------

    def _validate_transition(self, current: str, new: str):
        allowed = self.VALID_TRANSITIONS.get(current, [])
        if new not in allowed:
            raise ValueError(f"Invalid job transition: {current} → {new}")

    def _transition_job(self, job_id: int, new_state: str):
        job = self.job_repo.get_job(job_id)
        if not job:
            raise ValueError(f"Job {job_id} does not exist")

        current_state = job["status"]
        self._validate_transition(current_state, new_state)

        self.job_repo.update_status(job_id, new_state)

    # ----------------------------------------
    # Public Lifecycle Methods
    # ----------------------------------------

    def start_job(self, job_id: int):
        self._transition_job(job_id, "running")

    def complete_job(self, job_id: int):
        self._transition_job(job_id, "completed")

    def fail_job(self, job_id: int):
        self._transition_job(job_id, "failed")

    def retry_job(self, job_id: int):
        self._transition_job(job_id, "pending")
        self.job_repo.increment_retry(job_id)

        # Reset steps
        steps = self.step_repo.get_steps_for_job(job_id)
        for step in steps:
            self.step_repo.update_step_status(step["id"], "pending")

    # ----------------------------------------
    # Job Processing (Sequential)
    # ----------------------------------------

    def process_job(self, job_id: int, step_executor):
        """
        step_executor is a callable:
            result = step_executor(step_row)

        result should return:
            {
                "success": bool,
                "exit_code": int,
                "stdout": str,
                "stderr": str
            }
        """

        self.start_job(job_id)

        steps = self.step_repo.get_steps_for_job(job_id)

        for step in steps:
            step_id = step["id"]

            # Mark step running
            self.step_repo.update_step_status(step_id, "running")

            result = step_executor(step)

            if not result["success"]:
                self.step_repo.update_step_status(
                    step_id,
                    "failed",
                    exit_code=result.get("exit_code"),
                    stdout=result.get("stdout"),
                    stderr=result.get("stderr"),
                )
                self.fail_job(job_id)
                return

            # Step succeeded
            self.step_repo.update_step_status(
                step_id,
                "completed",
                exit_code=result.get("exit_code"),
                stdout=result.get("stdout"),
                stderr=result.get("stderr"),
            )

        # If all steps completed
        self.complete_job(job_id)