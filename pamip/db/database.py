import sqlite3
from pathlib import Path


class Database:
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._enable_foreign_keys()

    def _enable_foreign_keys(self):
        self.conn.execute("PRAGMA foreign_keys = ON;")

    def execute(self, sql: str, params: tuple = ()):
        cursor = self.conn.execute(sql, params)
        self.conn.commit()
        return cursor

    def executemany(self, sql: str, param_list: list[tuple]):
        cursor = self.conn.executemany(sql, param_list)
        self.conn.commit()
        return cursor

    def begin(self):
        self.conn.execute("BEGIN")

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        self.conn.close()