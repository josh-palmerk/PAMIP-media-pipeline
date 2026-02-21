import sqlite3
from pathlib import Path

DB_PATH = Path("pamip.db")


class Database:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._initialize()

    def get_connection(self):
        """
        Returns a new SQLite connection.
        Foreign keys are enforced.
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def _initialize(self):
        """
        Create tables if they do not exist.
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    error_message TEXT
                );
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS steps (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id INTEGER NOT NULL,
                    step_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at DATETIME,
                    ended_at DATETIME,
                    log TEXT,
                    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
                );
            """)

            conn.commit()