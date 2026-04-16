import csv
import time
from pathlib import Path

import pymysql

TABLES = [
    "users", "profiles", "subscriptions", "payments",
    "people", "content", "content_people", "seasons", "episodes",
    "watch_history", "my_list", "ratings",
]

BATCH_SIZE = 50000

_BOOL_COLUMNS = {
    "profiles": ["is_kids"],
    "subscriptions": ["auto_renew"],
    "content": ["is_active"],
    "watch_history": ["completed"],
}

_MYSQL_INDEXES = [
    ("users", "CREATE INDEX idx_users_status ON users (status)"),
    ("subscriptions", "CREATE INDEX idx_subscriptions_status_plan ON subscriptions (status, plan_name)"),
    ("payments", "CREATE INDEX idx_payments_status ON payments (status)"),
    ("content", "CREATE INDEX idx_content_type ON content (type)"),
    ("content", "CREATE INDEX idx_content_popularity ON content (popularity_score DESC)"),
    ("content", "CREATE INDEX idx_content_is_active ON content (is_active)"),
    ("content", "CREATE FULLTEXT INDEX idx_content_title_ft ON content (title)"),
    ("content", "CREATE INDEX idx_content_metadata_studio ON content ((CAST(metadata->>'$.studio' AS CHAR(100))))"),
    ("watch_history", "CREATE INDEX idx_wh_profile_started ON watch_history (profile_id, started_at DESC)"),
    ("watch_history", "CREATE INDEX idx_wh_started_at ON watch_history (started_at)"),
]

_MYSQL_INDEX_DROPS = [
    ("users", "idx_users_status"),
    ("subscriptions", "idx_subscriptions_status_plan"),
    ("payments", "idx_payments_status"),
    ("content", "idx_content_type"),
    ("content", "idx_content_popularity"),
    ("content", "idx_content_is_active"),
    ("content", "idx_content_title_ft"),
    ("content", "idx_content_metadata_studio"),
    ("watch_history", "idx_wh_profile_started"),
    ("watch_history", "idx_wh_started_at"),
]


class MySQLLoader:

    def __init__(
        self,
        data_dir: Path,
        host: str = "localhost",
        port: int = 3306,
        user: str = "root",
        password: str = "root123",
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
            local_infile=True,
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

        start = time.perf_counter()
        bool_cols = _BOOL_COLUMNS.get(table)

        if bool_cols:
            self._load_table_with_bool_conversion(conn, table, csv_path, bool_cols)
        else:
            self._load_table_direct(conn, table, csv_path)

        conn.commit()
        elapsed = time.perf_counter() - start
        print(f"  {table}: loaded ({elapsed:.1f}s)")

    def _load_table_direct(self, conn: pymysql.Connection, table: str, csv_path: Path) -> None:
        abs_path = csv_path.resolve().as_posix()
        with conn.cursor() as cur:
            sql = (
                f"LOAD DATA LOCAL INFILE '{abs_path}' "
                f"INTO TABLE {table} "
                f"FIELDS TERMINATED BY ',' "
                f"OPTIONALLY ENCLOSED BY '\"' "
                f"LINES TERMINATED BY '\\n' "
                f"IGNORE 1 LINES"
            )
            cur.execute(sql)

    def _load_table_with_bool_conversion(
        self, conn: pymysql.Connection, table: str, csv_path: Path, bool_cols: list[str]
    ) -> None:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            columns = next(reader)

        var_names = []
        set_clauses = []
        for col in columns:
            if col in bool_cols:
                var = f"@raw_{col}"
                var_names.append(var)
                set_clauses.append(f"{col} = ({var} = 'true')")
            else:
                var_names.append(col)

        abs_path = csv_path.resolve().as_posix()
        col_list = ", ".join(var_names)
        set_str = ", ".join(set_clauses)

        with conn.cursor() as cur:
            sql = (
                f"LOAD DATA LOCAL INFILE '{abs_path}' "
                f"INTO TABLE {table} "
                f"FIELDS TERMINATED BY ',' "
                f"OPTIONALLY ENCLOSED BY '\"' "
                f"LINES TERMINATED BY '\\n' "
                f"IGNORE 1 LINES "
                f"({col_list}) "
                f"SET {set_str}"
            )
            cur.execute(sql)

    def create_indexes(self) -> None:
        print("Creating MySQL indexes...")
        start = time.perf_counter()
        conn = pymysql.connect(**self.conn_params)
        try:
            with conn.cursor() as cur:
                for _table, sql in _MYSQL_INDEXES:
                    try:
                        cur.execute(sql)
                    except pymysql.err.OperationalError:
                        pass
            conn.commit()
        finally:
            conn.close()
        elapsed = time.perf_counter() - start
        print(f"MySQL: indexes created ({elapsed:.2f}s)")

    def drop_indexes(self) -> None:
        print("Dropping MySQL performance indexes...")
        conn = pymysql.connect(**self.conn_params)
        try:
            with conn.cursor() as cur:
                for table, idx_name in _MYSQL_INDEX_DROPS:
                    try:
                        cur.execute(f"DROP INDEX {idx_name} ON {table}")
                    except pymysql.err.OperationalError:
                        pass
            conn.commit()
        finally:
            conn.close()
