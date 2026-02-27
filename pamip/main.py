from db.database import Database
from db.schema import initialize_schema
from db.job_repository import JobRepository
from db.step_repository import StepRepository
from core.job_manager import JobManager
import time


def fake_step_executor(step):
    print(f"Executing step: {step['step_name']}")
    time.sleep(1)

    return {
        "success": True,
        "exit_code": 0,
        "stdout": "ok",
        "stderr": ""
    }


def main():
    db = Database("pamip.db")
    initialize_schema(db)

    job_repo = JobRepository(db)
    step_repo = StepRepository(db)
    manager = JobManager(job_repo, step_repo)

    job_id = manager.create_job_with_pipeline("/tmp/video.mp4")
    print("Created job:", job_id)

    manager.process_job(job_id, fake_step_executor)

    job = job_repo.get_job(job_id)
    print("Final job state:", dict(job))


if __name__ == "__main__":
    main()