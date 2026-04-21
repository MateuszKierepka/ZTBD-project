import copy
import csv
import time
import traceback
from pathlib import Path

import psycopg
import pymysql
from pymongo import MongoClient
from neo4j import GraphDatabase

from .base import VOLUME_PARAMS, BenchmarkContext
from . import ALL_SCENARIOS


class BenchmarkRunner:

    def __init__(self, volume: str, results_dir: Path):
        self.volume = volume
        self.results_dir = results_dir
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.connections: dict = {}
        self.ctx: BenchmarkContext | None = None

    def connect(self, databases: list[str] | None = None) -> None:
        targets = databases or ["postgres", "mysql", "mongo", "neo4j"]

        if "postgres" in targets:
            self.connections["postgres"] = psycopg.connect(
                "host=localhost port=5432 dbname=vod user=vod password=vod123"
            )

        if "mysql" in targets:
            self.connections["mysql"] = pymysql.connect(
                host="localhost", port=3306, user="vod",
                password="vod123", database="vod",
                autocommit=False,
            )

        if "mongo" in targets:
            client = MongoClient("localhost", 27017)
            self.connections["mongo"] = client["vod"]
            self._mongo_client = client

        if "neo4j" in targets:
            self.connections["neo4j"] = GraphDatabase.driver(
                "bolt://localhost:7687", auth=("neo4j", "vod12345")
            )

    def _create_connection(self, db_name: str):
        if db_name == "postgres":
            return psycopg.connect(
                "host=localhost port=5432 dbname=vod user=vod password=vod123"
            )
        elif db_name == "mysql":
            return pymysql.connect(
                host="localhost", port=3306, user="vod",
                password="vod123", database="vod",
                autocommit=False,
            )
        elif db_name == "mongo":
            client = MongoClient("localhost", 27017)
            return client["vod"]
        elif db_name == "neo4j":
            return GraphDatabase.driver(
                "bolt://localhost:7687", auth=("neo4j", "vod12345")
            )

    def _close_connection(self, db_name: str, conn) -> None:
        if db_name == "mongo":
            conn.client.close()
        else:
            conn.close()

    def build_context(self) -> None:
        pg = self.connections.get("postgres")
        if pg:
            conn = pg
        else:
            conn = psycopg.connect(
                "host=localhost port=5432 dbname=vod user=vod password=vod123"
            )

        max_ids = {}
        tables = [
            ("users", "user_id"), ("profiles", "profile_id"),
            ("subscriptions", "subscription_id"), ("payments", "payment_id"),
            ("people", "person_id"), ("content", "content_id"),
            ("seasons", "season_id"), ("episodes", "episode_id"),
            ("watch_history", "watch_id"), ("my_list", "list_id"),
            ("ratings", "rating_id"),
        ]
        for table, id_col in tables:
            row = conn.execute(f"SELECT MAX({id_col}) FROM {table}").fetchone()
            max_ids[table] = row[0] or 0

        if "postgres" not in self.connections:
            conn.close()

        self.ctx = BenchmarkContext(
            volume=self.volume,
            max_ids=max_ids,
            params=VOLUME_PARAMS[self.volume],
        )

    def run_all(
        self,
        with_indexes: bool,
        trials: int = 3,
        scenario_ids: list[str] | None = None,
    ) -> list[dict]:
        results = []
        self.ctx.with_indexes = with_indexes
        scenarios = ALL_SCENARIOS
        if scenario_ids:
            scenarios = [s for s in scenarios if s.id in scenario_ids]

        db_names = list(self.connections.keys())
        total = len(scenarios) * len(db_names) * trials
        done = 0

        for scenario in scenarios:
            for db_name in db_names:
                conn = self._create_connection(db_name)
                sc = copy.copy(scenario)

                self._flush_caches(db_name, conn)

                times = []
                for trial in range(1, trials + 1):
                    done += 1

                    try:
                        sc.setup(db_name, conn, self.ctx)
                    except Exception as exc:
                        print(f"  [{done}/{total}] {sc.id} | {db_name:<8} | "
                              f"trial {trial}: SETUP ERROR: {exc}")
                        traceback.print_exc()
                        self._try_rollback(db_name, conn)
                        continue

                    try:
                        start = time.perf_counter_ns()
                        sc.run(db_name, conn, self.ctx)
                        elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000
                    except Exception as exc:
                        print(f"  [{done}/{total}] {sc.id} | {db_name:<8} | "
                              f"trial {trial}: RUN ERROR: {exc}")
                        traceback.print_exc()
                        self._try_rollback(db_name, conn)
                        try:
                            sc.teardown(db_name, conn, self.ctx)
                        except Exception:
                            pass
                        continue

                    try:
                        sc.teardown(db_name, conn, self.ctx)
                    except Exception as exc:
                        print(f"  [{done}/{total}] {sc.id} | {db_name:<8} | "
                              f"trial {trial}: TEARDOWN ERROR: {exc}")

                    times.append(elapsed_ms)
                    results.append({
                        "scenario_id": sc.id,
                        "scenario_name": sc.name,
                        "category": sc.category,
                        "database": db_name,
                        "volume": self.volume,
                        "trial": trial,
                        "time_ms": round(elapsed_ms, 3),
                        "with_indexes": with_indexes,
                    })

                if times:
                    avg = sum(times) / len(times)
                    print(
                        f"  [{done}/{total}] {sc.id} | {db_name:<8} | "
                        f"avg={avg:.1f}ms  ({', '.join(f'{t:.1f}' for t in times)})"
                    )

                self._close_connection(db_name, conn)

        return results

    def _flush_caches(self, db_name: str, conn) -> None:
        try:
            if db_name == "postgres":
                conn.autocommit = True
                conn.execute("DISCARD ALL")
                conn.autocommit = False
            elif db_name == "mysql":
                cur = conn.cursor()
                cur.execute("FLUSH TABLES")
                cur.close()
            elif db_name == "mongo":
                for coll in ("users", "content", "watch_history", "ratings", "payments"):
                    try:
                        conn.command("planCacheClear", coll)
                    except Exception:
                        pass
            elif db_name == "neo4j":
                with conn.session() as s:
                    s.run("CALL db.clearQueryCaches()").consume()
        except Exception:
            pass

    def _try_rollback(self, db_name: str, conn) -> None:
        if db_name in ("postgres", "mysql") and hasattr(conn, "rollback"):
            try:
                conn.rollback()
            except Exception:
                pass

    def save_results(self, results: list[dict], filename: str) -> Path:
        filepath = self.results_dir / filename
        fieldnames = [
            "scenario_id", "scenario_name", "category", "database",
            "volume", "trial", "time_ms", "with_indexes",
        ]
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        print(f"Results saved to {filepath}")
        return filepath

    def close(self) -> None:
        for name, conn in self.connections.items():
            if name == "postgres":
                conn.close()
            elif name == "mysql":
                conn.close()
            elif name == "mongo":
                self._mongo_client.close()
            elif name == "neo4j":
                conn.close()
        self.connections.clear()
