from .database import Database


def initialize_schema(db: Database):
    db.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_path TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status IN (
            'pending','running','completed','failed','retrying'
        )),
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        started_at DATETIME,
        finished_at DATETIME,
        retry_count INTEGER NOT NULL DEFAULT 0
    );
    """)

    db.execute("""
    CREATE TABLE IF NOT EXISTS steps (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER NOT NULL,
        step_name TEXT NOT NULL,
        step_order INTEGER NOT NULL,
        status TEXT NOT NULL CHECK(status IN (
            'pending','running','completed','failed'
        )),
        started_at DATETIME,
        finished_at DATETIME,
        exit_code INTEGER,
        stdout TEXT,
        stderr TEXT,
        FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
    );
    """)

    db.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);")
    db.execute("CREATE INDEX IF NOT EXISTS idx_steps_job_id ON steps(job_id);")
    db.execute("CREATE INDEX IF NOT EXISTS idx_steps_job_order ON steps(job_id, step_order);")