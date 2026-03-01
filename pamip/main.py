from db.database import Database
from db.job_repository import JobRepository
from db.step_repository import StepRepository
from core.job_manager import JobManager
import db.schema as schema


def fake_step_executor(step):
    print(f"Executing step {step['step_name']}")

    # Simulate failure on first attempt of step2
    if step["step_name"] == "step2" and step["attempt_count"] == 0:
        return {
            "success": False,
            "exit_code": 1,
            "stdout": "",
            "stderr": "Simulated failure"
        }

    return {
        "success": True,
        "exit_code": 0,
        "stdout": "OK",
        "stderr": ""
    }


def main():
    db = Database("pamip.db")
    schema.initialize_schema(db)

    job_repo = JobRepository(db)
    step_repo = StepRepository(db)

    manager = JobManager(db, job_repo, step_repo)

    # ---- Create test job ----
    job_id = job_repo.create_job("Test Job")

    with db.transaction():
        step_repo.create_steps(
            job_id,
            [
                {"step_name": "step1", "max_attempts": 2},
                {"step_name": "step2", "max_attempts": 2},
            ]
        )

    print("Starting processing...")
    manager.process_job(job_id, fake_step_executor)

    print("Final job state:")
    job = job_repo.get_job(job_id)
    print(dict(job))

    steps = step_repo.get_steps_for_job(job_id)
    for step in steps:
        print(dict(step))


if __name__ == "__main__":
    main()