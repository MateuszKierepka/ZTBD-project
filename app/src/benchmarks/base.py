import random
import threading
from dataclasses import dataclass, field


VOLUME_PARAMS = {
    "small": {
        "batch_watch_history": 10_000,
        "batch_payments": 1_000,
        "batch_ratings": 500,
        "batch_people": 200,
        "batch_users_delete": 50,
        "search_term": "Interface",
        "old_history_cutoff": "2021-06-01",
    },
    "medium": {
        "batch_watch_history": 100_000,
        "batch_payments": 10_000,
        "batch_ratings": 5_000,
        "batch_people": 1_000,
        "batch_users_delete": 500,
        "search_term": "Interface",
        "old_history_cutoff": "2022-06-01",
    },
    "large": {
        "batch_watch_history": 500_000,
        "batch_payments": 50_000,
        "batch_ratings": 25_000,
        "batch_people": 5_000,
        "batch_users_delete": 2_000,
        "search_term": "Interface",
        "old_history_cutoff": "2023-06-01",
    },
}


@dataclass
class BenchmarkContext:
    volume: str
    max_ids: dict[str, int]
    params: dict[str, object]
    with_indexes: bool = False
    _counter: int = field(default=0, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def next_id(self) -> int:
        with self._lock:
            self._counter += 1
            return self._counter

    def test_id(self, table: str) -> int:
        return self.max_ids[table] + 100_000 + self.next_id()

    def random_id(self, table: str) -> int:
        return random.randint(1, self.max_ids[table])


class BaseScenario:
    id: str = ""
    name: str = ""
    category: str = ""

    def setup(self, db_type: str, conn, ctx: BenchmarkContext) -> None:
        method = getattr(self, f"setup_{db_type}", None)
        if method:
            method(conn, ctx)

    def run(self, db_type: str, conn, ctx: BenchmarkContext) -> None:
        getattr(self, f"run_{db_type}")(conn, ctx)

    def teardown(self, db_type: str, conn, ctx: BenchmarkContext) -> None:
        method = getattr(self, f"teardown_{db_type}", None)
        if method:
            method(conn, ctx)

    def __repr__(self) -> str:
        return f"{self.id}: {self.name}"
