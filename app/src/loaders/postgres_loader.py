import csv
import time
from pathlib import Path

import psycopg

TABLES = [
    "users", "profiles", "subscriptions", "payments",
    "people", "content", "content_people", "seasons", "episodes",
    "watch_history", "my_list", "ratings",
]

SEQUENCES = {
    "users": ("users_user_id_seq", "user_id"),
    "profiles": ("profiles_profile_id_seq", "profile_id"),
    "subscriptions": ("subscriptions_subscription_id_seq", "subscription_id"),
    "payments": ("payments_payment_id_seq", "payment_id"),
    "people": ("people_person_id_seq", "person_id"),
    "content": ("content_content_id_seq", "content_id"),
    "seasons": ("seasons_season_id_seq", "season_id"),
    "episodes": ("episodes_episode_id_seq", "episode_id"),
    "watch_history": ("watch_history_watch_id_seq", "watch_id"),
    "my_list": ("my_list_list_id_seq", "list_id"),
    "ratings": ("ratings_rating_id_seq", "rating_id"),
}

_PG_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_users_status ON users (status)",
    "CREATE INDEX IF NOT EXISTS idx_profiles_user_id ON profiles (user_id)",
    "CREATE INDEX IF NOT EXISTS idx_subscriptions_user_id ON subscriptions (user_id)",
    "CREATE INDEX IF NOT EXISTS idx_subscriptions_status_plan ON subscriptions (status, plan_name)",
    "CREATE INDEX IF NOT EXISTS idx_payments_subscription_id ON payments (subscription_id)",
    "CREATE INDEX IF NOT EXISTS idx_payments_status ON payments (status)",
    "CREATE INDEX IF NOT EXISTS idx_content_type ON content (type)",
    "CREATE INDEX IF NOT EXISTS idx_content_popularity ON content (popularity_score DESC)",
    "CREATE INDEX IF NOT EXISTS idx_content_active_popular ON content (popularity_score DESC) WHERE is_active = TRUE",
    "CREATE INDEX IF NOT EXISTS idx_content_title_trgm ON content USING GIN (title gin_trgm_ops)",
    "CREATE INDEX IF NOT EXISTS idx_content_metadata_gin ON content USING GIN (metadata)",
    "CREATE INDEX IF NOT EXISTS idx_content_people_person_id ON content_people (person_id)",
    "CREATE INDEX IF NOT EXISTS idx_wh_profile_started ON watch_history (profile_id, started_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_wh_content_id ON watch_history (content_id)",
    "CREATE INDEX IF NOT EXISTS idx_wh_started_at ON watch_history (started_at)",
    "CREATE INDEX IF NOT EXISTS idx_ratings_content_id ON ratings (content_id)",
    "CREATE INDEX IF NOT EXISTS idx_my_list_content_id ON my_list (content_id)",
]

_PG_INDEX_NAMES = [
    "idx_users_status",
    "idx_profiles_user_id",
    "idx_subscriptions_user_id",
    "idx_subscriptions_status_plan",
    "idx_payments_subscription_id",
    "idx_payments_status",
    "idx_content_type",
    "idx_content_popularity",
    "idx_content_active_popular",
    "idx_content_title_trgm",
    "idx_content_metadata_gin",
    "idx_content_people_person_id",
    "idx_wh_profile_started",
    "idx_wh_content_id",
    "idx_wh_started_at",
    "idx_ratings_content_id",
    "idx_my_list_content_id",
]


class PostgresLoader:

    def __init__(
        self,
        data_dir: Path,
        host: str = "localhost",
        port: int = 5432,
        dbname: str = "vod",
        user: str = "vod",
        password: str = "vod123",
    ):
        self.data_dir = data_dir
        self.conn_string = f"host={host} port={port} dbname={dbname} user={user} password={password}"

    def load_all(self) -> None:
        print("Loading data into PostgreSQL...")
        total_start = time.perf_counter()

        with psycopg.connect(self.conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute("SET maintenance_work_mem = '512MB'")
                cur.execute("SET work_mem = '64MB'")
            conn.commit()

            self._truncate_all(conn)

            for table in TABLES:
                self._load_table(conn, table)

            self._reset_sequences(conn)

        elapsed = time.perf_counter() - total_start
        print(f"PostgreSQL: loaded successfully ({elapsed:.2f}s)")

    def _truncate_all(self, conn: psycopg.Connection) -> None:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {', '.join(TABLES)} CASCADE")
        conn.commit()

    def _load_table(self, conn: psycopg.Connection, table: str) -> None:
        csv_path = self.data_dir / f"{table}.csv"
        if not csv_path.exists():
            print(f"  WARNING: {table}.csv not found, skipping")
            return

        start = time.perf_counter()

        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            columns = next(reader)

        col_str = ", ".join(columns)
        copy_sql = (
            f"COPY {table} ({col_str}) FROM STDIN "
            f"WITH (FORMAT csv, HEADER true, NULL '')"
        )

        with conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                with open(csv_path, "rb") as f:
                    while chunk := f.read(8 * 1024 * 1024):
                        copy.write(chunk)
        conn.commit()
        elapsed = time.perf_counter() - start
        print(f"  {table}: loaded ({elapsed:.1f}s)")

    def create_indexes(self) -> None:
        print("Creating PostgreSQL indexes...")
        start = time.perf_counter()
        with psycopg.connect(self.conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
                for sql in _PG_INDEXES:
                    cur.execute(sql)
            conn.commit()
        elapsed = time.perf_counter() - start
        print(f"PostgreSQL: indexes created ({elapsed:.2f}s)")

    def drop_indexes(self) -> None:
        print("Dropping PostgreSQL performance indexes...")
        with psycopg.connect(self.conn_string) as conn:
            with conn.cursor() as cur:
                for name in _PG_INDEX_NAMES:
                    cur.execute(f"DROP INDEX IF EXISTS {name}")
            conn.commit()

    def _reset_sequences(self, conn: psycopg.Connection) -> None:
        with conn.cursor() as cur:
            for table, (seq_name, pk_col) in SEQUENCES.items():
                cur.execute(
                    f"SELECT setval('{seq_name}', "
                    f"COALESCE((SELECT MAX({pk_col}) FROM {table}), 1))"
                )
        conn.commit()
