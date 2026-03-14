import csv
import time
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
        scenarios = ALL_SCENARIOS
        if scenario_ids:
            scenarios = [s for s in scenarios if s.id in scenario_ids]

        total = len(scenarios) * len(self.connections) * trials
        done = 0

        for scenario in scenarios:
            for db_name, conn in self.connections.items():
                times = []
                for trial in range(1, trials + 1):
                    done += 1
                    scenario.setup(db_name, conn, self.ctx)

                    start = time.perf_counter_ns()
                    scenario.run(db_name, conn, self.ctx)
                    elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000

                    scenario.teardown(db_name, conn, self.ctx)
                    times.append(elapsed_ms)

                    results.append({
                        "scenario_id": scenario.id,
                        "scenario_name": scenario.name,
                        "category": scenario.category,
                        "database": db_name,
                        "volume": self.volume,
                        "trial": trial,
                        "time_ms": round(elapsed_ms, 3),
                        "with_indexes": with_indexes,
                    })

                avg = sum(times) / len(times)
                print(
                    f"  [{done}/{total}] {scenario.id} | {db_name:<8} | "
                    f"avg={avg:.1f}ms  ({', '.join(f'{t:.1f}' for t in times)})"
                )

        return results

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
