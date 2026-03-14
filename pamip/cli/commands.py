"""
cli/commands.py
Command-line interface for PAMIP.
Provides job monitoring and control commands via argparse.

Commands:
    pamip list              — list all jobs with status (FR-19)
    pamip show <job_id>     — show detailed job and step info (FR-20)
    pamip retry <job_id>    — retry a failed job (FR-21)
    pamip stats             — show job counts by status (FR-22)
"""

import argparse
import sys

from db.database import Database
from db.job_repository import JobRepository
from db.step_repository import StepRepository
from db import schema


# ----------------------------------------
# Formatting Helpers
# ----------------------------------------

def _divider(widths: list[int]) -> str:
    """
    _divider
    Builds a table row divider string matching the given column widths.

    Args:
        widths (list[int]) — list of column widths

    Returns:
        str — divider string e.g. "+----+----------+"
    """
    return "+" + "+".join("-" * (w + 2) for w in widths) + "+"


def _row(values: list[str], widths: list[int]) -> str:
    """
    _row
    Formats a single table row with padded columns.

    Args:
        values (list[str]) — cell values
        widths (list[int]) — column widths

    Returns:
        str — formatted row string e.g. "| 1  | pending  |"
    """
    cells = " | ".join(str(v).ljust(w) for v, w in zip(values, widths))
    return f"| {cells} |"


def _print_table(headers: list[str], rows: list[list[str]]):
    """
    _print_table
    Prints a formatted ASCII table to stdout.

    Args:
        headers (list[str])       — column header labels
        rows    (list[list[str]]) — table data rows
    """
    # Column width is the max of header length and any cell in that column
    widths = [
        max(len(headers[i]), max((len(str(row[i])) for row in rows), default=0))
        for i in range(len(headers))
    ]

    div = _divider(widths)
    print(div)
    print(_row(headers, widths))
    print(div)
    for row in rows:
        print(_row(row, widths))
    print(div)


# ----------------------------------------
# Command Handlers
# ----------------------------------------

def cmd_list(job_repo: JobRepository):
    """
    cmd_list
    Lists all jobs ordered by creation time. (FR-19)

    Args:
        job_repo (JobRepository) — job persistence layer
    """
    jobs = job_repo.list_jobs()

    if not jobs:
        print("No jobs found.")
        return

    headers = ["ID", "STATUS", "FILE", "CREATED", "RETRIES"]
    rows = [
        [job.id, job.status, job.file_path, job.created_at, job.retry_count]
        for job in jobs
    ]
    _print_table(headers, rows)


def cmd_show(job_repo: JobRepository, step_repo: StepRepository, job_id: int):
    """
    cmd_show
    Displays detailed information for a single job and its steps. (FR-20)

    Args:
        job_repo  (JobRepository)  — job persistence layer
        step_repo (StepRepository) — step persistence layer
        job_id    (int)            — ID of the job to display
    """
    job = job_repo.get_job(job_id)
    if not job:
        print(f"Job {job_id} not found.")
        return

    # Print job summary
    print(f"\nJob {job.id}")
    print(f"  File:       {job.file_path}")
    print(f"  Status:     {job.status}")
    print(f"  Retries:    {job.retry_count}")
    print(f"  Created:    {job.created_at}")
    print(f"  Started:    {job.started_at or '—'}")
    print(f"  Finished:   {job.finished_at or '—'}")

    # Print step table
    steps = step_repo.get_steps_for_job(job_id)
    if not steps:
        print("\n  No steps found.")
        return

    print()
    headers = ["STEP", "NAME", "STATUS", "ATTEMPTS", "EXIT CODE", "STARTED", "FINISHED"]
    rows = [
        [
            step.step_order,
            step.step_name,
            step.status,
            f"{step.attempt_count}/{step.max_attempts}",
            step.exit_code if step.exit_code is not None else "—",
            step.started_at or "—",
            step.finished_at or "—",
        ]
        for step in steps
    ]
    _print_table(headers, rows)

    # Print stdout/stderr for failed steps
    for step in steps:
        if step.status == "failed" and (step.stdout or step.stderr):
            print(f"\n  Step {step.step_order} ({step.step_name}) output:")
            if step.stdout:
                print(f"    stdout: {step.stdout.strip()}")
            if step.stderr:
                print(f"    stderr: {step.stderr.strip()}")


def cmd_retry(db: Database, job_repo: JobRepository, step_repo: StepRepository, job_id: int):
    """
    cmd_retry
    Resets a failed job and its incomplete steps to pending for re-processing. (FR-21)

    Args:
        db        (Database)       — database connection for transactions
        job_repo  (JobRepository)  — job persistence layer
        step_repo (StepRepository) — step persistence layer
        job_id    (int)            — ID of the job to retry
    """
    job = job_repo.get_job(job_id)
    if not job:
        print(f"Job {job_id} not found.")
        return

    if job.status != "failed":
        print(f"Job {job_id} is '{job.status}' — only failed jobs can be retried.")
        return

    with db.transaction():
        # Reset non-completed steps to pending
        for step in step_repo.get_steps_for_job(job_id):
            if step.status != "completed":
                step_repo.update_step_status(step.id, "pending")

        # Transition job: failed -> pending
        job_repo.update_status(job_id, "pending")

    print(f"Job {job_id} queued for retry.")


def cmd_stats(job_repo: JobRepository):
    """
    cmd_stats
    Displays job counts grouped by status. (FR-22)

    Args:
        job_repo (JobRepository) — job persistence layer
    """
    jobs = job_repo.list_jobs()

    counts = {"pending": 0, "running": 0, "completed": 0, "failed": 0}
    for job in jobs:
        if job.status in counts:
            counts[job.status] += 1

    headers = ["STATUS", "COUNT"]
    rows = [[status, count] for status, count in counts.items()]
    _print_table(headers, rows)


# ----------------------------------------
# Entry Point
# ----------------------------------------

def main():
    """
    main
    Parses CLI arguments and dispatches to the appropriate command handler.
    Connects to the database and initializes repositories before dispatch.
    """
    parser = argparse.ArgumentParser(
        prog="pamip",
        description="PAMIP — Pipeline for Automated Media Ingestion & Processing"
    )
    subparsers = parser.add_subparsers(dest="command")

    # pamip list
    subparsers.add_parser("list", help="List all jobs")

    # pamip show <job_id>
    show_parser = subparsers.add_parser("show", help="Show job details")
    show_parser.add_argument("job_id", type=int, help="Job ID to inspect")

    # pamip retry <job_id>
    retry_parser = subparsers.add_parser("retry", help="Retry a failed job")
    retry_parser.add_argument("job_id", type=int, help="Job ID to retry")

    # pamip stats
    subparsers.add_parser("stats", help="Show job statistics")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    # Connect to database
    db = Database("pamip.db")
    schema.initialize_schema(db)
    job_repo  = JobRepository(db)
    step_repo = StepRepository(db)

    # Dispatch
    if args.command == "list":
        cmd_list(job_repo)

    elif args.command == "show":
        cmd_show(job_repo, step_repo, args.job_id)

    elif args.command == "retry":
        cmd_retry(db, job_repo, step_repo, args.job_id)

    elif args.command == "stats":
        cmd_stats(job_repo)

    db.close()


if __name__ == "__main__":
    main()
