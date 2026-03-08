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
                    while chunk := f.read(65536):
                        copy.write(chunk)
        conn.commit()

    def _reset_sequences(self, conn: psycopg.Connection) -> None:
        with conn.cursor() as cur:
            for table, (seq_name, pk_col) in SEQUENCES.items():
                cur.execute(
                    f"SELECT setval('{seq_name}', "
                    f"COALESCE((SELECT MAX({pk_col}) FROM {table}), 1))"
                )
        conn.commit()
