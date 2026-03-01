import sqlite3
from pathlib import Path
from contextlib import contextmanager


class Database:
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

        # Enable FK enforcement
        self.conn.execute("PRAGMA foreign_keys = ON;")

        # Optional but recommended
        self.conn.execute("PRAGMA journal_mode = WAL;")

        # Manual transaction control
        self.conn.isolation_level = None

    # ------------------------------
    # Core Execution (NO auto commit)
    # ------------------------------

    def execute(self, sql: str, params: tuple = ()):
        return self.conn.execute(sql, params)

    def executemany(self, sql: str, param_list):
        return self.conn.executemany(sql, param_list)

    def fetchone(self, sql: str, params: tuple = ()):
        return self.conn.execute(sql, params).fetchone()

    def fetchall(self, sql: str, params: tuple = ()):
        return self.conn.execute(sql, params).fetchall()

    # ------------------------------
    # Transaction Manager
    # ------------------------------

    @contextmanager
    def transaction(self):
        try:
            self.conn.execute("BEGIN")
            yield
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def close(self):
        self.conn.close()