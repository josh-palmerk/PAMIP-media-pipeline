"""
job_manager.py
Orchestration layer for job lifecycle and sequential step execution.
Coordinates between JobRepository and StepRepository; owns all state
transition logic and retry behavior.
"""

from db.database import Database
from db.job_repository import JobRepository
from db.step_repository import StepRepository


class JobManager:
    """
    JobManager
    Manages job state transitions and drives sequential pipeline execution.
    Enforces valid state transitions and delegates all persistence to the
    injected repository objects.
    """

    # Valid state transitions: key -> list of allowed next states
    VALID_TRANSITIONS = {
        "pending":   ["running"],
        "running":   ["completed", "failed"],
        "failed":    ["pending"],
        "completed": []
    }

    def __init__(self, db: Database, job_repo: JobRepository, step_repo: StepRepository):
        self.db = db
        self.job_repo = job_repo
        self.step_repo = step_repo

    # ----------------------------------------
    # State Validation
    # ----------------------------------------

    def _validate_transition(self, current: str, new: str):
        """
        _validate_transition
        Raises ValueError if transitioning from current -> new is not allowed.
        """
        allowed = self.VALID_TRANSITIONS.get(current, [])
        if new not in allowed:
            raise ValueError(f"Invalid job transition: {current} → {new}")

    def _transition_job_no_tx(self, job_id: int, new_state: str):
        """
        _transition_job_no_tx
        Validates and applies a state transition for a job.
        Must be called inside an existing transaction — does not open one.
        Raises ValueError if the job does not exist or the transition is invalid.
        """
        job = self.job_repo.get_job(job_id)
        if not job:
            raise ValueError(f"Job {job_id} does not exist")

        self._validate_transition(job.status, new_state)
        self.job_repo.update_status(job_id, new_state)

    # ----------------------------------------
    # Public Lifecycle Methods
    # ----------------------------------------

    def start_job(self, job_id: int):
        """
        start_job
        Transitions a job from pending -> running.
        """
        with self.db.transaction():
            self._transition_job_no_tx(job_id, "running")

    def complete_job(self, job_id: int):
        """
        complete_job
        Transitions a job from running -> completed.
        Raises ValueError if any steps are not yet completed.
        """
        with self.db.transaction():
            steps = self.step_repo.get_steps_for_job(job_id)
            if any(step.status != "completed" for step in steps):
                raise ValueError("Cannot complete job: not all steps completed")

            self._transition_job_no_tx(job_id, "completed")

    def fail_job(self, job_id: int):
        """
        fail_job
        Transitions a job from running -> failed.
        """
        with self.db.transaction():
            self._transition_job_no_tx(job_id, "failed")

    # ----------------------------------------
    # Sequential Processing
    # ----------------------------------------

    def process_job(self, job_id: int, step_executor):
        """
        process_job
        Executes all steps for a job sequentially.
        Handles per-step retries and resets the step loop on retry.

        step_executor: callable(step: Step) -> dict
            Must return a dict with keys:
                success   (bool)
                exit_code (int, optional)
                stdout    (str, optional)
                stderr    (str, optional)

        Terminal outcomes:
            All steps succeed       -> job transitions to completed
            Step exhausts retries   -> job transitions to failed
        """

        # Transition job: pending -> running
        self.start_job(job_id)

        while True:  # outer loop allows restart after a retry

            steps = self.step_repo.get_steps_for_job(job_id)
            restart_required = False

            for step in steps:

                # Skip already completed steps (important on retry restarts)
                if step.status == "completed":
                    continue

                # ---- Mark step running ----
                with self.db.transaction():
                    self.step_repo.update_step_status(step.id, "running")

                # ---- Execute outside DB transaction ----
                result = step_executor(step)

                # ---- Persist result ----
                with self.db.transaction():

                    # Re-fetch to get the current attempt_count from DB
                    fresh_step = self.step_repo.get_step(step.id)
                    attempts = fresh_step.attempt_count + 1

                    if not result["success"]:
                        # Record failure and increment attempt counter
                        self.step_repo.update_step_status(
                            step.id,
                            "failed",
                            exit_code=result.get("exit_code"),
                            stdout=result.get("stdout"),
                            stderr=result.get("stderr"),
                            attempt_count=attempts,
                        )

                        if attempts < fresh_step.max_attempts:
                            # ---- RETRY CASE ----

                            # Increment job-level retry counter
                            self.job_repo.increment_retry(job_id)

                            # Reset all non-completed steps to pending
                            for s in steps:
                                if s.status != "completed":
                                    self.step_repo.update_step_status(s.id, "pending")

                            restart_required = True
                            break  # break for-loop, outer while will restart

                        else:
                            # ---- TERMINAL FAILURE ----
                            self._transition_job_no_tx(job_id, "failed")
                            return

                    else:
                        # ---- SUCCESS CASE ----
                        self.step_repo.update_step_status(
                            step.id,
                            "completed",
                            exit_code=result.get("exit_code"),
                            stdout=result.get("stdout"),
                            stderr=result.get("stderr"),
                        )

            if restart_required:
                continue  # restart step execution from the beginning

            # All steps completed successfully
            break

        # Transition job: running -> completed
        self.complete_job(job_id)
