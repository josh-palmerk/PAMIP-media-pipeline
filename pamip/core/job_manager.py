class JobManager:
    VALID_TRANSITIONS = {
        "pending": ["running"],
        "running": ["completed", "failed"],
        "failed": ["pending"],
        "completed": []
    }

    def __init__(self, db, job_repo, step_repo):
        self.db = db
        self.job_repo = job_repo
        self.step_repo = step_repo

    # ----------------------------------------
    # State Validation
    # ----------------------------------------

    def _validate_transition(self, current: str, new: str):
        allowed = self.VALID_TRANSITIONS.get(current, [])
        if new not in allowed:
            raise ValueError(f"Invalid job transition: {current} → {new}")

    def _transition_job_no_tx(self, job_id: int, new_state: str):
        job = self.job_repo.get_job(job_id)
        if not job:
            raise ValueError(f"Job {job_id} does not exist")

        self._validate_transition(job["status"], new_state)
        self.job_repo.update_status(job_id, new_state)

    # ----------------------------------------
    # Public Lifecycle Methods
    # ----------------------------------------

    def start_job(self, job_id: int):
        with self.db.transaction():
            self._transition_job_no_tx(job_id, "running")

    def complete_job(self, job_id: int):
        with self.db.transaction():
            steps = self.step_repo.get_steps_for_job(job_id)
            if any(step["status"] != "completed" for step in steps):
                raise ValueError("Cannot complete job: not all steps completed")

            self._transition_job_no_tx(job_id, "completed")

    def fail_job(self, job_id: int):
        with self.db.transaction():
            self._transition_job_no_tx(job_id, "failed")

    # ----------------------------------------
    # Sequential Processing (Safe Version)
    # ----------------------------------------
    def process_job(self, job_id: int, step_executor):
        """
        Execute all steps sequentially.
        Job remains in 'running' state until either:
        - all steps succeed  -> completed
        - a step exhausts retries -> failed
        """

        # Transition job: pending -> running
        self.start_job(job_id)

        while True:  # allows restart of step loop after retry

            steps = self.step_repo.get_steps_for_job(job_id)
            restart_required = False

            for step in steps:
                step_id = step["id"]

                # Skip already completed steps (important for retries)
                if step["status"] == "completed":
                    continue

                # ---- Mark step running ----
                with self.db.transaction():
                    self.step_repo.update_step_status(step_id, "running")

                # ---- Execute outside DB transaction ----
                result = step_executor(step)

                # ---- Persist result ----
                with self.db.transaction():

                    fresh_step = self.step_repo.get_step(step_id)
                    attempts = fresh_step["attempt_count"] + 1

                    if not result["success"]:
                        # Record failure and increment attempt counter
                        self.step_repo.update_step_status(
                            step_id,
                            "failed",
                            exit_code=result.get("exit_code"),
                            stdout=result.get("stdout"),
                            stderr=result.get("stderr"),
                            attempt_count=attempts,
                        )

                        if attempts < fresh_step["max_attempts"]:
                            # ---- RETRY CASE ----

                            # Increment job-level retry counter
                            self.job_repo.increment_retry(job_id)

                            # Reset all incomplete steps to pending
                            for s in steps:
                                if s["status"] != "completed":
                                    self.step_repo.update_step_status(
                                        s["id"], "pending"
                                    )

                            restart_required = True
                            break  # break for-loop, restart while-loop

                        else:
                            # ---- TERMINAL FAILURE ----
                            self._transition_job_no_tx(job_id, "failed")
                            return

                    else:
                        # ---- SUCCESS CASE ----
                        self.step_repo.update_step_status(
                            step_id,
                            "completed",
                            exit_code=result.get("exit_code"),
                            stdout=result.get("stdout"),
                            stderr=result.get("stderr"),
                        )

            if restart_required:
                continue  # restart step execution from beginning

            # If we reach here, all steps succeeded
            break

        # Transition job: running -> completed
        self.complete_job(job_id)