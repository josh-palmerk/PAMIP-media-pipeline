"""
pipeline/engine.py
Bridges the job manager and the executor.
Resolves step handlers, builds commands, and drives execution for each step.
"""

from jobs.models import Job, Step
from pipeline.steps import get_step_handler
from tools.executor import run_process


class PipelineEngine:
    """
    PipelineEngine
    Executes pipeline steps for a given job by resolving each step's handler,
    building the appropriate command, and invoking the executor.

    Intended to be passed as the step_executor callable to
    JobManager.process_job(). Each call receives a Step and returns a
    result dict that JobManager uses to record outcomes and drive retries.

    Usage:
        engine = PipelineEngine(step_configs, output_dir)
        manager.process_job(job_id, engine.execute_step)
    """

    def __init__(self, step_configs: list, output_dir: str):
        """
        __init__
        Args:
            step_configs (list[StepConfig]) — pipeline step definitions from config,
                                              used to look up timeout_seconds per step
            output_dir   (str)              — directory where processed files are written,
                                              passed to handlers so output never lands in
                                              the watch directory
        """
        # Build a lookup of step_name -> timeout_seconds for quick access during execution
        self._timeouts:   dict[str, int] = {
            s.step_name: s.timeout_seconds for s in step_configs
        }
        self.output_dir = output_dir

    def execute_step(self, step: Step, job: Job) -> dict:
        """
        execute_step
        Resolves the handler for the given step, builds the command, and
        runs it via the executor. Returns a result dict compatible with
        JobManager.process_job().

        Fails the step immediately if no handler is registered for the
        step name.

        Args:
            step  (Step) — the step to execute
            job   (Job)  — the parent job, passed to the handler for context

        Returns:
            dict with keys:
                success   (bool)
                exit_code (int)
                stdout    (str)
                stderr    (str)
        """
        # Resolve handler — fail the step if none is registered
        try:
            handler = get_step_handler(step.step_name)
        except KeyError as e:
            return {
                "success":   False,
                "exit_code": -1,
                "stdout":    "",
                "stderr":    str(e),
            }

        # Build command from handler and run it
        command         = handler(job.file_path, self.output_dir, job)
        timeout_seconds = self._timeouts.get(step.step_name, 60)
        result          = run_process(command, timeout_seconds)

        return {
            "success":   result.success,
            "exit_code": result.exit_code,
            "stdout":    result.stdout,
            "stderr":    result.stderr,
        }
