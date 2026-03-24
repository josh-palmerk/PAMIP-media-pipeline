"""
main.py
Application entry point for PAMIP.
Running with no arguments starts the worker loop.
Running with a subcommand (list, show, retry, stats) runs the CLI.

Usage:
    python main.py                  — start the worker
    python main.py list             — list all jobs
    python main.py show <job_id>    — show job details
    python main.py retry <job_id>   — retry a failed job
    python main.py stats            — show job statistics
"""

import sys
import signal
import logging
import argparse
from pathlib import Path

from config import load_config
from db.database import Database
from db.job_repository import JobRepository
from db.step_repository import StepRepository
from db import schema
from ingestion.watcher import FileWatcher
from pipeline.engine import PipelineEngine
from worker.loop import WorkerLoop
from cli.commands import cmd_list, cmd_show, cmd_retry, cmd_stats


# ----------------------------------------
# Logging Setup
# ----------------------------------------

def _setup_logging():
    """
    _setup_logging
    Configures logging to write to both console and a rotating log file.
    Log file is written to logs/pamip.log, created if it does not exist.
    """
    Path("logs").mkdir(exist_ok=True)

    log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("logs/pamip.log"),
        ]
    )


# ----------------------------------------
# Startup Validation
# ----------------------------------------

def _warn_missing_directories(config):
    """
    _warn_missing_directories
    Prints warnings if the configured watch or output directories do not exist.
    Does not create them — these are user-managed paths.

    Args:
        config (Config) — loaded application configuration
    """
    if not Path(config.watch_directory).exists():
        print(f"[warning] Watch directory does not exist: {config.watch_directory}")

    if not Path(config.output_directory).exists():
        print(f"[warning] Output directory does not exist: {config.output_directory}")


# ----------------------------------------
# Worker Entry Point
# ----------------------------------------

def _start_worker(config, db, job_repo, step_repo):
    """
    _start_worker
    Builds and starts the worker loop. Blocks until SIGINT or SIGTERM.

    Args:
        config    (Config)          — loaded application configuration
        db        (Database)        — shared database connection
        job_repo  (JobRepository)   — job persistence layer
        step_repo (StepRepository)  — step persistence layer
    """
    engine = PipelineEngine(config.pipeline)

    watcher = FileWatcher(
        watch_dir=          config.watch_directory,
        allowed_extensions= config.allowed_extensions,
    )

    loop = WorkerLoop(
        db=                  db,
        job_repo=            job_repo,
        step_repo=           step_repo,
        engine=              engine,
        watcher=             watcher,
        poll_interval=       config.poll_interval_seconds,
        max_concurrent_jobs= config.max_concurrent_jobs,
    )

    # Graceful shutdown on SIGINT (Ctrl+C) or SIGTERM
    def _handle_signal(sig, frame):
        print("\n[main] Shutdown signal received. Stopping worker...")
        loop.stop()

    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    loop.start()


# ----------------------------------------
# Argument Parsing
# ----------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    """
    _build_parser
    Builds and returns the top-level argument parser with all subcommands.

    Returns:
        argparse.ArgumentParser
    """
    parser = argparse.ArgumentParser(
        prog="pamip",
        description="PAMIP — Pipeline for Automated Media Ingestion & Processing"
    )
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("list",  help="List all jobs")
    subparsers.add_parser("stats", help="Show job statistics")

    show_parser = subparsers.add_parser("show", help="Show job details")
    show_parser.add_argument("job_id", type=int, help="Job ID to inspect")

    retry_parser = subparsers.add_parser("retry", help="Retry a failed job")
    retry_parser.add_argument("job_id", type=int, help="Job ID to retry")

    return parser


# ----------------------------------------
# Main
# ----------------------------------------

def main():
    """
    main
    Parses arguments, loads config, connects to the database, and either
    starts the worker loop or dispatches a CLI command.
    """
    _setup_logging()

    parser = _build_parser()
    args   = parser.parse_args()

    # Load config — creates default if missing
    try:
        config = load_config()
    except ValueError as e:
        print(f"[error] Configuration error: {e}")
        sys.exit(1)

    # Connect to database and initialize schema
    db = Database("pamip.db")
    schema.initialize_schema(db)
    job_repo  = JobRepository(db)
    step_repo = StepRepository(db)

    # Dispatch CLI command or start worker
    if args.command == "list":
        cmd_list(job_repo)

    elif args.command == "show":
        cmd_show(job_repo, step_repo, args.job_id)

    elif args.command == "retry":
        cmd_retry(db, job_repo, step_repo, args.job_id)

    elif args.command == "stats":
        cmd_stats(job_repo)

    else:
        # No subcommand — start the worker
        _warn_missing_directories(config)
        _start_worker(config, db, job_repo, step_repo)

    db.close()


if __name__ == "__main__":
    main()
