from dataclasses import dataclass
import sqlite3

@dataclass
class Job:
    id:             int
    file_path:      str
    status:         str
    retry_count:    int
    created_at:     str
    updated_at:     str
    started_at:     str | None
    finished_at:    str | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Job":
        """ Returns a Job object from a given sqlite3 row. """
        return cls(
            id=             row["id"],
            file_path=      row["file_path"],
            status=         row["status"],
            retry_count=    row["retry_count"],
            created_at=     row["created_at"],
            updated_at=     row["updated_at"],
            started_at=     row["started_at"],
            finished_at=    row["finished_at"],
        )


@dataclass
class Step:
    id:             int
    job_id:         int
    step_name:      str
    step_order:     int
    status:         str
    attempt_count:  int
    max_attempts:   int
    started_at:     str | None
    finished_at:    str | None
    exit_code:      int | None
    stdout:         str | None
    stderr:         str | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Job":
        """ Returns a Job object from a given sqlite3 row. """
        return cls(
            id=             row["id"],
            job_id=         row["job_id"],
            step_name=      row["step_name"],
            step_order=     row["step_order"],
            status=         row["status"],
            attempt_count=  row["attempt_count"],
            max_attempts=   row["max_attempts"],
            started_at=     row["started_at"],
            finished_at=    row["finished_at"],
            exit_code=      row["exit_code"],
            stdout=         row["stdout"],
            stderr=         row["stderr"],
        )
