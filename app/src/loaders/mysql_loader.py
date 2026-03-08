import csv
import time
from pathlib import Path

import pymysql

TABLES = [
    "users", "profiles", "subscriptions", "payments",
    "people", "content", "content_people", "seasons", "episodes",
    "watch_history", "my_list", "ratings",
]

BATCH_SIZE = 5000


class MySQLLoader:

    def __init__(
        self,
        data_dir: Path,
        host: str = "localhost",
        port: int = 3306,
        user: str = "vod",
        password: str = "vod123",
        database: str = "vod",
    ):
        self.data_dir = data_dir
        self.conn_params = dict(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset="utf8mb4",
        )

    def load_all(self) -> None:
        print("Loading data into MySQL...")
        total_start = time.perf_counter()

        conn = pymysql.connect(**self.conn_params)
        try:
            self._disable_checks(conn)
            self._truncate_all(conn)

            for table in TABLES:
                self._load_table(conn, table)

            self._enable_checks(conn)
        finally:
            conn.close()

        elapsed = time.perf_counter() - total_start
        print(f"MySQL: loaded successfully ({elapsed:.2f}s)")

    def _disable_checks(self, conn: pymysql.Connection) -> None:
        with conn.cursor() as cur:
            cur.execute("SET FOREIGN_KEY_CHECKS = 0")
            cur.execute("SET UNIQUE_CHECKS = 0")
            cur.execute("SET AUTOCOMMIT = 0")

    def _enable_checks(self, conn: pymysql.Connection) -> None:
        with conn.cursor() as cur:
            cur.execute("SET FOREIGN_KEY_CHECKS = 1")
            cur.execute("SET UNIQUE_CHECKS = 1")
            cur.execute("SET AUTOCOMMIT = 1")
        conn.commit()

    def _truncate_all(self, conn: pymysql.Connection) -> None:
        with conn.cursor() as cur:
            for table in reversed(TABLES):
                cur.execute(f"TRUNCATE TABLE {table}")
        conn.commit()

    def _load_table(self, conn: pymysql.Connection, table: str) -> None:
        csv_path = self.data_dir / f"{table}.csv"
        if not csv_path.exists():
            print(f"  WARNING: {table}.csv not found, skipping")
            return

        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            columns = next(reader)

            col_str = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))
            sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"

            batch = []
            for row in reader:
                batch.append([_convert(v) for v in row])

                if len(batch) >= BATCH_SIZE:
                    with conn.cursor() as cur:
                        cur.executemany(sql, batch)
                    conn.commit()
                    batch = []

            if batch:
                with conn.cursor() as cur:
                    cur.executemany(sql, batch)
                conn.commit()


def _convert(value: str):
    if value == "":
        return None
    if value == "true":
        return 1
    if value == "false":
        return 0
    return value
