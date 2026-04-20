import csv
import time
from pathlib import Path

from neo4j import GraphDatabase

BATCH_SIZE = 25000


class Neo4jLoader:

    def __init__(
        self,
        data_dir: Path,
        uri: str = "bolt://localhost:7687",
        user: str = "neo4j",
        password: str = "vod12345",
    ):
        self.data_dir = data_dir
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def load_all(self) -> None:
        print("Loading data into Neo4j...")
        total_start = time.perf_counter()

        self._clear_database()
        self._create_constraints()

        for label, method in [
            ("users", self._load_user_nodes),
            ("profiles", self._load_profile_nodes),
            ("subscriptions", self._load_subscription_nodes),
            ("payments", self._load_payment_nodes),
            ("people", self._load_person_nodes),
            ("content", self._load_content_nodes),
            ("genres", self._load_genre_nodes),
            ("seasons", self._load_season_nodes),
            ("episodes", self._load_episode_nodes),
        ]:
            start = time.perf_counter()
            method()
            print(f"  {label}: loaded ({time.perf_counter() - start:.1f}s)")

        print("  Nodes created. Loading relationships...")

        for label, method in [
            ("user->profile", self._create_user_profile_rels),
            ("user->subscription", self._create_user_subscription_rels),
            ("subscription->payment", self._create_subscription_payment_rels),
            ("content->genre", self._create_content_genre_rels),
            ("content->season", self._create_content_season_rels),
            ("season->episode", self._create_season_episode_rels),
            ("content_people", self._create_content_people_rels),
            ("watch_history", self._create_watched_rels),
            ("my_list", self._create_my_list_rels),
            ("ratings", self._create_rated_rels),
        ]:
            start = time.perf_counter()
            method()
            print(f"  {label}: loaded ({time.perf_counter() - start:.1f}s)")

        elapsed = time.perf_counter() - total_start
        print(f"Neo4j: loaded successfully ({elapsed:.2f}s)")

    def create_indexes(self) -> None:
        indexes = [
            "CREATE INDEX IF NOT EXISTS FOR (u:User) ON (u.status)",
            "CREATE INDEX IF NOT EXISTS FOR (u:User) ON (u.country_code)",
            "CREATE INDEX IF NOT EXISTS FOR (c:Content) ON (c.type)",
            "CREATE INDEX IF NOT EXISTS FOR (c:Content) ON (c.popularity_score)",
            "CREATE INDEX IF NOT EXISTS FOR (c:Content) ON (c.is_active)",
            "CREATE TEXT INDEX IF NOT EXISTS FOR (c:Content) ON (c.title)",
            "CREATE TEXT INDEX IF NOT EXISTS FOR (c:Content) ON (c.metadata)",
            "CREATE INDEX IF NOT EXISTS FOR (s:Subscription) ON (s.status)",
            "CREATE INDEX IF NOT EXISTS FOR (p:Payment) ON (p.status)",
        ]
        with self.driver.session() as session:
            for idx in indexes:
                session.run(idx)
        print("Neo4j: performance indexes created")

    def close(self) -> None:
        self.driver.close()

    def _clear_database(self) -> None:
        with self.driver.session() as session:
            while True:
                result = session.run(
                    "MATCH ()-[r]->() WITH r LIMIT 50000 DELETE r RETURN count(*) AS deleted"
                )
                if result.single()["deleted"] == 0:
                    break
            while True:
                result = session.run(
                    "MATCH (n) WITH n LIMIT 50000 DELETE n RETURN count(*) AS deleted"
                )
                if result.single()["deleted"] == 0:
                    break

    def _create_constraints(self) -> None:
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (u:User) REQUIRE u.user_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Profile) REQUIRE p.profile_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Content) REQUIRE c.content_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Season) REQUIRE s.season_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (e:Episode) REQUIRE e.episode_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE p.person_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (g:Genre) REQUIRE g.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Subscription) REQUIRE s.subscription_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Payment) REQUIRE p.payment_id IS UNIQUE",
        ]
        with self.driver.session() as session:
            for c in constraints:
                session.run(c)

    def _load_user_nodes(self) -> None:
        rows = [
            {
                "user_id": int(r["user_id"]),
                "email": r["email"],
                "first_name": r["first_name"],
                "last_name": r["last_name"],
                "country_code": r["country_code"],
                "status": r["status"],
                "created_at": r["created_at"],
            }
            for r in self._read_csv("users.csv")
        ]
        self._batch_write(
            "UNWIND $rows AS r "
            "CREATE (:User {user_id: r.user_id, email: r.email, "
            "first_name: r.first_name, last_name: r.last_name, "
            "country_code: r.country_code, status: r.status, "
            "created_at: r.created_at})",
            rows,
        )

    def _load_profile_nodes(self) -> None:
        rows = [
            {
                "profile_id": int(r["profile_id"]),
                "name": r["name"],
                "is_kids": r["is_kids"] == "true",
                "maturity_rating": r["maturity_rating"],
                "language": r["language"],
            }
            for r in self._read_csv("profiles.csv")
        ]
        self._batch_write(
            "UNWIND $rows AS r "
            "CREATE (:Profile {profile_id: r.profile_id, name: r.name, "
            "is_kids: r.is_kids, maturity_rating: r.maturity_rating, "
            "language: r.language})",
            rows,
        )

    def _load_subscription_nodes(self) -> None:
        rows = [
            {
                "subscription_id": int(r["subscription_id"]),
                "plan_name": r["plan_name"],
                "price_monthly": float(r["price_monthly"]),
                "max_streams": int(r["max_streams"]),
                "max_resolution": r["max_resolution"],
                "status": r["status"],
                "start_date": r["start_date"],
                "end_date": r["end_date"] or None,
                "auto_renew": r["auto_renew"] == "true",
            }
            for r in self._read_csv("subscriptions.csv")
        ]
        self._batch_write(
            "UNWIND $rows AS r "
            "CREATE (:Subscription {subscription_id: r.subscription_id, "
            "plan_name: r.plan_name, price_monthly: r.price_monthly, "
            "max_streams: r.max_streams, max_resolution: r.max_resolution, "
            "status: r.status, start_date: r.start_date, "
            "end_date: r.end_date, auto_renew: r.auto_renew})",
            rows,
        )

    def _load_payment_nodes(self) -> None:
        rows = [
            {
                "payment_id": int(r["payment_id"]),
                "amount": float(r["amount"]),
                "currency": r["currency"],
                "payment_method": r["payment_method"],
                "status": r["status"],
                "paid_at": r["paid_at"] or None,
            }
            for r in self._read_csv("payments.csv")
        ]
        self._batch_write(
            "UNWIND $rows AS r "
            "CREATE (:Payment {payment_id: r.payment_id, "
            "amount: r.amount, currency: r.currency, "
            "payment_method: r.payment_method, status: r.status, "
            "paid_at: r.paid_at})",
            rows,
        )

    def _load_person_nodes(self) -> None:
        rows = [
            {
                "person_id": int(r["person_id"]),
                "first_name": r["first_name"],
                "last_name": r["last_name"],
                "birth_date": r["birth_date"] or None,
                "nationality": r["nationality"] or None,
            }
            for r in self._read_csv("people.csv")
        ]
        self._batch_write(
            "UNWIND $rows AS r "
            "CREATE (:Person {person_id: r.person_id, "
            "first_name: r.first_name, last_name: r.last_name, "
            "birth_date: r.birth_date, nationality: r.nationality})",
            rows,
        )

    def _load_content_nodes(self) -> None:
        rows = [
            {
                "content_id": int(r["content_id"]),
                "title": r["title"],
                "type": r["type"],
                "release_date": r["release_date"] or None,
                "duration_minutes": int(r["duration_minutes"]) if r["duration_minutes"] else None,
                "maturity_rating": r["maturity_rating"],
                "avg_rating": float(r["avg_rating"]) if r["avg_rating"] else 0.0,
                "total_views": int(r["total_views"]) if r["total_views"] else 0,
                "popularity_score": float(r["popularity_score"]) if r["popularity_score"] else 0.0,
                "is_active": r["is_active"] == "true",
                "metadata": r["metadata"] if r["metadata"] else None,
                "created_at": r["created_at"],
            }
            for r in self._read_csv("content.csv")
        ]
        self._batch_write(
            "UNWIND $rows AS r "
            "CREATE (:Content {content_id: r.content_id, title: r.title, "
            "type: r.type, release_date: r.release_date, "
            "duration_minutes: r.duration_minutes, "
            "maturity_rating: r.maturity_rating, avg_rating: r.avg_rating, "
            "total_views: r.total_views, popularity_score: r.popularity_score, "
            "is_active: r.is_active, metadata: r.metadata, "
            "created_at: r.created_at})",
            rows,
        )

    def _load_genre_nodes(self) -> None:
        genres: set[str] = set()
        for r in self._read_csv("content.csv"):
            if r["genres"]:
                genres.update(r["genres"].split(","))

        rows = [{"name": g} for g in sorted(genres)]
        self._batch_write(
            "UNWIND $rows AS r CREATE (:Genre {name: r.name})",
            rows,
        )

    def _load_season_nodes(self) -> None:
        rows = [
            {
                "season_id": int(r["season_id"]),
                "season_number": int(r["season_number"]),
                "title": r["title"],
                "release_date": r["release_date"] or None,
            }
            for r in self._read_csv("seasons.csv")
        ]
        self._batch_write(
            "UNWIND $rows AS r "
            "CREATE (:Season {season_id: r.season_id, "
            "season_number: r.season_number, title: r.title, "
            "release_date: r.release_date})",
            rows,
        )

    def _load_episode_nodes(self) -> None:
        rows = [
            {
                "episode_id": int(r["episode_id"]),
                "episode_number": int(r["episode_number"]),
                "title": r["title"],
                "duration_minutes": int(r["duration_minutes"]),
                "release_date": r["release_date"] or None,
            }
            for r in self._read_csv("episodes.csv")
        ]
        self._batch_write(
            "UNWIND $rows AS r "
            "CREATE (:Episode {episode_id: r.episode_id, "
            "episode_number: r.episode_number, title: r.title, "
            "duration_minutes: r.duration_minutes, "
            "release_date: r.release_date})",
            rows,
        )

    def _create_user_profile_rels(self) -> None:
        rows = [
            {"user_id": int(r["user_id"]), "profile_id": int(r["profile_id"])}
            for r in self._read_csv("profiles.csv")
        ]
        self._batch_write(
            "UNWIND $rows AS r "
            "MATCH (u:User {user_id: r.user_id}) "
            "MATCH (p:Profile {profile_id: r.profile_id}) "
            "CREATE (u)-[:HAS_PROFILE]->(p)",
            rows,
        )

    def _create_user_subscription_rels(self) -> None:
        rows = [
            {"user_id": int(r["user_id"]), "subscription_id": int(r["subscription_id"])}
            for r in self._read_csv("subscriptions.csv")
        ]
        self._batch_write(
            "UNWIND $rows AS r "
            "MATCH (u:User {user_id: r.user_id}) "
            "MATCH (s:Subscription {subscription_id: r.subscription_id}) "
            "CREATE (u)-[:HAS_SUBSCRIPTION]->(s)",
            rows,
        )

    def _create_subscription_payment_rels(self) -> None:
        rows = [
            {"subscription_id": int(r["subscription_id"]), "payment_id": int(r["payment_id"])}
            for r in self._read_csv("payments.csv")
        ]
        self._batch_write(
            "UNWIND $rows AS r "
            "MATCH (s:Subscription {subscription_id: r.subscription_id}) "
            "MATCH (p:Payment {payment_id: r.payment_id}) "
            "CREATE (s)-[:HAS_PAYMENT]->(p)",
            rows,
        )

    def _create_content_genre_rels(self) -> None:
        rows = []
        for r in self._read_csv("content.csv"):
            if r["genres"]:
                cid = int(r["content_id"])
                for genre in r["genres"].split(","):
                    rows.append({"content_id": cid, "genre": genre})
        self._batch_write(
            "UNWIND $rows AS r "
            "MATCH (c:Content {content_id: r.content_id}) "
            "MATCH (g:Genre {name: r.genre}) "
            "CREATE (c)-[:HAS_GENRE]->(g)",
            rows,
        )

    def _create_content_season_rels(self) -> None:
        rows = [
            {"content_id": int(r["content_id"]), "season_id": int(r["season_id"])}
            for r in self._read_csv("seasons.csv")
        ]
        self._batch_write(
            "UNWIND $rows AS r "
            "MATCH (c:Content {content_id: r.content_id}) "
            "MATCH (s:Season {season_id: r.season_id}) "
            "CREATE (c)-[:HAS_SEASON]->(s)",
            rows,
        )

    def _create_season_episode_rels(self) -> None:
        rows = [
            {"season_id": int(r["season_id"]), "episode_id": int(r["episode_id"])}
            for r in self._read_csv("episodes.csv")
        ]
        self._batch_write(
            "UNWIND $rows AS r "
            "MATCH (s:Season {season_id: r.season_id}) "
            "MATCH (e:Episode {episode_id: r.episode_id}) "
            "CREATE (s)-[:HAS_EPISODE]->(e)",
            rows,
        )

    def _create_content_people_rels(self) -> None:
        acted = []
        directed = []
        wrote = []
        for r in self._read_csv("content_people.csv"):
            entry = {
                "content_id": int(r["content_id"]),
                "person_id": int(r["person_id"]),
                "character_name": r["character_name"] or None,
                "billing_order": int(r["billing_order"]) if r["billing_order"] else None,
            }
            role = r["role"]
            if role == "actor":
                acted.append(entry)
            elif role == "director":
                directed.append(entry)
            elif role == "writer":
                wrote.append(entry)

        self._batch_write(
            "UNWIND $rows AS r "
            "MATCH (p:Person {person_id: r.person_id}) "
            "MATCH (c:Content {content_id: r.content_id}) "
            "CREATE (p)-[:ACTED_IN {character_name: r.character_name, "
            "billing_order: r.billing_order}]->(c)",
            acted,
        )
        self._batch_write(
            "UNWIND $rows AS r "
            "MATCH (p:Person {person_id: r.person_id}) "
            "MATCH (c:Content {content_id: r.content_id}) "
            "CREATE (p)-[:DIRECTED]->(c)",
            directed,
        )
        self._batch_write(
            "UNWIND $rows AS r "
            "MATCH (p:Person {person_id: r.person_id}) "
            "MATCH (c:Content {content_id: r.content_id}) "
            "CREATE (p)-[:WROTE]->(c)",
            wrote,
        )

    def _create_watched_rels(self) -> None:
        self._batch_write_streaming(
            "UNWIND $rows AS r "
            "MATCH (p:Profile {profile_id: r.profile_id}) "
            "MATCH (c:Content {content_id: r.content_id}) "
            "CREATE (p)-[:WATCHED {started_at: r.started_at, "
            "progress_percent: r.progress_percent, completed: r.completed}]->(c)",
            (
                {
                    "profile_id": int(r["profile_id"]),
                    "content_id": int(r["content_id"]),
                    "started_at": r["started_at"],
                    "progress_percent": float(r["progress_percent"]),
                    "completed": r["completed"] == "true",
                }
                for r in self._read_csv("watch_history.csv")
            ),
        )

    def _create_my_list_rels(self) -> None:
        self._batch_write_streaming(
            "UNWIND $rows AS r "
            "MATCH (p:Profile {profile_id: r.profile_id}) "
            "MATCH (c:Content {content_id: r.content_id}) "
            "CREATE (p)-[:ADDED_TO_LIST {added_at: r.added_at}]->(c)",
            (
                {
                    "profile_id": int(r["profile_id"]),
                    "content_id": int(r["content_id"]),
                    "added_at": r["added_at"],
                }
                for r in self._read_csv("my_list.csv")
            ),
        )

    def _create_rated_rels(self) -> None:
        self._batch_write_streaming(
            "UNWIND $rows AS r "
            "MATCH (p:Profile {profile_id: r.profile_id}) "
            "MATCH (c:Content {content_id: r.content_id}) "
            "CREATE (p)-[:RATED {score: r.score, review_text: r.review_text, "
            "created_at: r.created_at}]->(c)",
            (
                {
                    "profile_id": int(r["profile_id"]),
                    "content_id": int(r["content_id"]),
                    "score": int(r["score"]),
                    "review_text": r["review_text"] or None,
                    "created_at": r["created_at"],
                }
                for r in self._read_csv("ratings.csv")
            ),
        )

    def _batch_write(self, query: str, rows: list[dict]) -> None:
        with self.driver.session() as session:
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i : i + BATCH_SIZE]
                session.run(query, rows=batch)

    def _batch_write_streaming(self, query: str, rows_iter) -> None:
        batch: list[dict] = []
        with self.driver.session() as session:
            for row in rows_iter:
                batch.append(row)
                if len(batch) >= BATCH_SIZE:
                    session.run(query, rows=batch)
                    batch = []
            if batch:
                session.run(query, rows=batch)

    def _read_csv(self, filename: str):
        filepath = self.data_dir / filename
        with open(filepath, "r", encoding="utf-8") as f:
            yield from csv.DictReader(f)
