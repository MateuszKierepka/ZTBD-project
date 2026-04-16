import csv
import json
import time
from pathlib import Path

from pymongo import MongoClient, ASCENDING, DESCENDING

BATCH_SIZE = 25000


class MongoLoader:

    def __init__(
        self,
        data_dir: Path,
        host: str = "localhost",
        port: int = 27017,
        database: str = "vod",
    ):
        self.data_dir = data_dir
        self.client = MongoClient(host, port)
        self.db = self.client[database]

    def load_all(self) -> None:
        print("Loading data into MongoDB...")
        total_start = time.perf_counter()

        self._drop_collections()
        self._load_users()
        self._load_content()
        self._load_watch_history()
        self._load_ratings()
        self._load_payments()
        self._load_my_list()

        elapsed = time.perf_counter() - total_start
        print(f"MongoDB: loaded successfully ({elapsed:.2f}s)")

    def close(self) -> None:
        self.client.close()

    def _drop_collections(self) -> None:
        for name in ["users", "content", "watch_history", "ratings", "payments", "my_list"]:
            self.db.drop_collection(name)

    def _load_users(self) -> None:
        users: dict[int, dict] = {}
        for row in self._read_csv("users.csv"):
            uid = int(row["user_id"])
            users[uid] = {
                "_id": uid,
                "email": row["email"],
                "password_hash": row["password_hash"],
                "first_name": row["first_name"],
                "last_name": row["last_name"],
                "date_of_birth": row["date_of_birth"],
                "country_code": row["country_code"],
                "phone": row["phone"] or None,
                "status": row["status"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "profiles": [],
                "subscription": None,
            }

        for row in self._read_csv("profiles.csv"):
            uid = int(row["user_id"])
            if uid in users:
                users[uid]["profiles"].append({
                    "profile_id": int(row["profile_id"]),
                    "name": row["name"],
                    "avatar_url": row["avatar_url"] or None,
                    "is_kids": row["is_kids"] == "true",
                    "maturity_rating": row["maturity_rating"],
                    "language": row["language"],
                    "created_at": row["created_at"],
                })

        for row in self._read_csv("subscriptions.csv"):
            uid = int(row["user_id"])
            if uid in users and row["status"] == "active":
                users[uid]["subscription"] = {
                    "subscription_id": int(row["subscription_id"]),
                    "plan_name": row["plan_name"],
                    "price_monthly": float(row["price_monthly"]),
                    "max_streams": int(row["max_streams"]),
                    "max_resolution": row["max_resolution"],
                    "max_profiles": int(row["max_profiles"]),
                    "status": row["status"],
                    "start_date": row["start_date"],
                    "end_date": row["end_date"] or None,
                    "auto_renew": row["auto_renew"] == "true",
                }

        self._batch_insert("users", list(users.values()))

    def _load_content(self) -> None:
        people: dict[int, dict] = {}
        for row in self._read_csv("people.csv"):
            pid = int(row["person_id"])
            people[pid] = {
                "first_name": row["first_name"],
                "last_name": row["last_name"],
            }

        contents: dict[int, dict] = {}
        for row in self._read_csv("content.csv"):
            cid = int(row["content_id"])
            genres_str = row["genres"]
            contents[cid] = {
                "_id": cid,
                "title": row["title"],
                "description": row["description"] or None,
                "type": row["type"],
                "release_date": row["release_date"] or None,
                "duration_minutes": int(row["duration_minutes"]) if row["duration_minutes"] else None,
                "maturity_rating": row["maturity_rating"],
                "genres": genres_str.split(",") if genres_str else [],
                "poster_url": row["poster_url"] or None,
                "trailer_url": row["trailer_url"] or None,
                "avg_rating": float(row["avg_rating"]) if row["avg_rating"] else 0.0,
                "total_views": int(row["total_views"]) if row["total_views"] else 0,
                "popularity_score": float(row["popularity_score"]) if row["popularity_score"] else 0.0,
                "country_of_origin": row["country_of_origin"] or None,
                "original_language": row["original_language"] or None,
                "is_active": row["is_active"] == "true",
                "metadata": json.loads(row["metadata"]) if row["metadata"] else {},
                "created_at": row["created_at"],
                "cast": [],
                "seasons": [],
            }

        for row in self._read_csv("content_people.csv"):
            cid = int(row["content_id"])
            pid = int(row["person_id"])
            person = people.get(pid, {})
            if cid in contents:
                contents[cid]["cast"].append({
                    "person_id": pid,
                    "first_name": person.get("first_name", ""),
                    "last_name": person.get("last_name", ""),
                    "role": row["role"],
                    "character_name": row["character_name"] or None,
                    "billing_order": int(row["billing_order"]) if row["billing_order"] else None,
                })

        seasons: dict[int, dict] = {}
        for row in self._read_csv("seasons.csv"):
            sid = int(row["season_id"])
            seasons[sid] = {
                "season_id": sid,
                "content_id": int(row["content_id"]),
                "season_number": int(row["season_number"]),
                "title": row["title"],
                "release_date": row["release_date"] or None,
                "episodes": [],
            }

        for row in self._read_csv("episodes.csv"):
            sid = int(row["season_id"])
            if sid in seasons:
                seasons[sid]["episodes"].append({
                    "episode_id": int(row["episode_id"]),
                    "episode_number": int(row["episode_number"]),
                    "title": row["title"],
                    "description": row["description"] or None,
                    "duration_minutes": int(row["duration_minutes"]),
                    "release_date": row["release_date"] or None,
                    "video_url": row["video_url"] or None,
                })

        for s in seasons.values():
            cid = s["content_id"]
            if cid in contents:
                season_doc = {k: v for k, v in s.items() if k != "content_id"}
                contents[cid]["seasons"].append(season_doc)

        for c in contents.values():
            c["seasons"].sort(key=lambda x: x["season_number"])
            for s in c["seasons"]:
                s["episodes"].sort(key=lambda x: x["episode_number"])

        self._batch_insert("content", list(contents.values()))

    def _load_watch_history(self) -> None:
        self._load_flat_collection(
            "watch_history",
            "watch_history.csv",
            lambda row: {
                "_id": int(row["watch_id"]),
                "profile_id": int(row["profile_id"]),
                "content_id": int(row["content_id"]),
                "episode_id": int(row["episode_id"]) if row["episode_id"] else None,
                "started_at": row["started_at"],
                "progress_percent": float(row["progress_percent"]),
                "completed": row["completed"] == "true",
            },
        )

    def _load_ratings(self) -> None:
        self._load_flat_collection(
            "ratings",
            "ratings.csv",
            lambda row: {
                "_id": int(row["rating_id"]),
                "profile_id": int(row["profile_id"]),
                "content_id": int(row["content_id"]),
                "score": int(row["score"]),
                "review_text": row["review_text"] or None,
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            },
        )

    def _load_payments(self) -> None:
        self._load_flat_collection(
            "payments",
            "payments.csv",
            lambda row: {
                "_id": int(row["payment_id"]),
                "subscription_id": int(row["subscription_id"]),
                "amount": float(row["amount"]),
                "currency": row["currency"],
                "payment_method": row["payment_method"],
                "transaction_id": row["transaction_id"],
                "status": row["status"],
                "paid_at": row["paid_at"] or None,
                "created_at": row["created_at"],
            },
        )

    def _load_my_list(self) -> None:
        self._load_flat_collection(
            "my_list",
            "my_list.csv",
            lambda row: {
                "_id": int(row["list_id"]),
                "profile_id": int(row["profile_id"]),
                "content_id": int(row["content_id"]),
                "added_at": row["added_at"],
                "sort_order": int(row["sort_order"]),
            },
        )

    def create_indexes(self) -> None:
        self.db["users"].create_index("email", unique=True)
        self.db["users"].create_index("status")
        self.db["users"].create_index("profiles.profile_id")

        self.db["content"].create_index("type")
        self.db["content"].create_index("genres")
        self.db["content"].create_index([("popularity_score", DESCENDING)])
        self.db["content"].create_index("is_active")
        self.db["content"].create_index([("title", "text")])

        self.db["watch_history"].create_index([("profile_id", ASCENDING), ("started_at", DESCENDING)])
        self.db["watch_history"].create_index("content_id")

        self.db["ratings"].create_index([("profile_id", ASCENDING), ("content_id", ASCENDING)], unique=True)
        self.db["ratings"].create_index("content_id")

        self.db["payments"].create_index("subscription_id")
        self.db["payments"].create_index("status")

        self.db["my_list"].create_index([("profile_id", ASCENDING), ("content_id", ASCENDING)], unique=True)

    def _load_flat_collection(self, collection: str, filename: str, transform) -> None:
        batch: list[dict] = []
        for row in self._read_csv(filename):
            batch.append(transform(row))
            if len(batch) >= BATCH_SIZE:
                self.db[collection].insert_many(batch, ordered=False)
                batch = []
        if batch:
            self.db[collection].insert_many(batch, ordered=False)

    def _batch_insert(self, collection: str, docs: list[dict]) -> None:
        for i in range(0, len(docs), BATCH_SIZE):
            self.db[collection].insert_many(docs[i : i + BATCH_SIZE], ordered=False)

    def _read_csv(self, filename: str):
        filepath = self.data_dir / filename
        with open(filepath, "r", encoding="utf-8") as f:
            yield from csv.DictReader(f)
