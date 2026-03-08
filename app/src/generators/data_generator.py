import csv
import random
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

from faker import Faker

from src.config import PLATFORM_END, PLATFORM_START, VolumeConfig

SUBSCRIPTION_PLANS = {
    "Basic": {"price": 29.99, "max_streams": 1, "resolution": "720p", "max_profiles": 2},
    "Standard": {"price": 43.99, "max_streams": 2, "resolution": "1080p", "max_profiles": 5},
    "Premium": {"price": 59.99, "max_streams": 4, "resolution": "4K", "max_profiles": 5},
}

GENRES = [
    "Action", "Comedy", "Drama", "Horror", "Sci-Fi", "Thriller",
    "Romance", "Documentary", "Animation", "Crime", "Fantasy", "Adventure",
]
MATURITY_RATINGS = ["ALL", "7+", "13+", "16+", "18+"]
LANGUAGES = ["pl", "en", "de", "fr", "es"]
COUNTRIES = ["PL", "US", "GB", "DE", "FR", "ES", "IT", "CZ", "SK", "UA"]
COUNTRY_WEIGHTS = [40, 15, 10, 8, 7, 5, 5, 4, 3, 3]
COUNTRY_PHONE_PREFIXES = {
    "PL": "48", "US": "1", "GB": "44", "DE": "49", "FR": "33",
    "ES": "34", "IT": "39", "CZ": "420", "SK": "421", "UA": "380",
}
PAYMENT_METHODS = ["card", "blik", "paypal", "transfer"]
EMAIL_DOMAINS = ["gmail.com", "wp.pl", "onet.pl", "o2.pl", "interia.pl", "yahoo.com", "outlook.com"]

USER_STATUSES = ["active", "active", "active", "active", "inactive", "suspended", "deleted"]
SUB_STATUSES_PREV = ["cancelled", "expired"]
PAYMENT_STATUSES = ["completed", "completed", "completed", "completed", "pending", "failed", "refunded"]

PROFILE_NAMES = ["Główny", "Dzieci", "Gość", "Partner", "Rodzina"]
PROFILE_COUNT_WEIGHTS = [15, 30, 30, 15, 10]
PLAN_WEIGHTS = [30, 45, 25]
SCORE_WEIGHTS = [2, 3, 5, 8, 12, 15, 20, 18, 10, 7]
SEASON_COUNT_WEIGHTS = [20, 25, 20, 15, 8, 5, 4, 3]


class DataGenerator:

    def __init__(self, config: VolumeConfig, output_dir: Path, seed: int = 42):
        self.config = config
        self.output_dir = output_dir
        self.seed = seed

        self.fake = Faker(["pl_PL", "en_US"])
        Faker.seed(seed)
        random.seed(seed)

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._init_pools()

        self._profile_count = 0
        self._subscription_count = 0
        self._series_ids: list[int] = []
        self._movie_ids: list[int] = []
        self._season_map: list[tuple[int, int]] = []
        self._content_episode_map: dict[int, list[int]] = {}

    def _init_pools(self) -> None:
        self._first_names = [self.fake.first_name() for _ in range(500)]
        self._last_names = [self.fake.last_name() for _ in range(500)]
        self._bios = [self.fake.paragraph(nb_sentences=3) for _ in range(200)]
        self._descriptions = [self.fake.paragraph(nb_sentences=5) for _ in range(500)]
        self._reviews = [self.fake.paragraph(nb_sentences=2) for _ in range(300)]
        self._titles = [self.fake.catch_phrase() for _ in range(2000)]
        self._ep_titles = [self.fake.sentence(nb_words=4).rstrip(".") for _ in range(1000)]

    def generate_all(self) -> None:
        print(f"Generating data (volume={self.config.name}, seed={self.seed})...")

        self._generate_users()
        self._generate_people()
        self._generate_content()

        self._generate_profiles()
        self._generate_subscriptions()
        self._generate_payments()

        self._generate_content_people()
        self._generate_seasons()
        self._generate_episodes()

        self._generate_watch_history()
        self._generate_my_list()
        self._generate_ratings()

        print(f"Data generated successfully. 12 CSV files saved to {self.output_dir.resolve()}")

    def _generate_users(self) -> None:
        n = self.config.users
        headers = [
            "user_id", "email", "password_hash", "first_name", "last_name",
            "date_of_birth", "country_code", "phone", "status",
            "created_at", "updated_at",
        ]

        def rows():
            for uid in range(1, n + 1):
                first = random.choice(self._first_names)
                last = random.choice(self._last_names)
                email = f"{first.lower()}.{last.lower()}.{uid}@{random.choice(EMAIL_DOMAINS)}"
                pw_hash = f"$2b$12${uuid.uuid4().hex[:53]}"
                dob = _random_date(date(1960, 1, 1), date(2006, 1, 1))
                country = random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0]
                prefix = COUNTRY_PHONE_PREFIXES.get(country, "48")
                digits = self.fake.numerify("#########")
                phone = f"+{prefix} {digits[:3]} {digits[3:6]} {digits[6:]}" if random.random() < 0.7 else ""
                status = random.choice(USER_STATUSES)
                created = _random_datetime(PLATFORM_START, PLATFORM_END)
                updated = _random_datetime(created, PLATFORM_END)
                yield [
                    uid, email, pw_hash, first, last, dob, country,
                    phone, status, _fmt_ts(created), _fmt_ts(updated),
                ]

        self._write_csv("users.csv", headers, rows())

    def _generate_profiles(self) -> None:
        headers = [
            "profile_id", "user_id", "name", "avatar_url", "is_kids",
            "maturity_rating", "language", "created_at",
        ]
        pid = 0

        def rows():
            nonlocal pid
            for uid in range(1, self.config.users + 1):
                n_profiles = random.choices([1, 2, 3, 4, 5], weights=PROFILE_COUNT_WEIGHTS)[0]
                for i in range(n_profiles):
                    pid += 1
                    name = PROFILE_NAMES[i] if i < len(PROFILE_NAMES) else f"Profil {i + 1}"
                    avatar = f"https://avatar.example.com/{pid}.jpg" if random.random() < 0.6 else ""
                    is_kids = i == 1 and n_profiles > 1 and random.random() < 0.4
                    maturity = "ALL" if is_kids else random.choice(MATURITY_RATINGS)
                    lang = random.choices(LANGUAGES, weights=[60, 25, 5, 5, 5])[0]
                    created = _random_datetime(PLATFORM_START, PLATFORM_END)
                    yield [
                        pid, uid, name, avatar, _bool(is_kids),
                        maturity, lang, _fmt_ts(created),
                    ]

        self._write_csv("profiles.csv", headers, rows())
        self._profile_count = pid

    def _generate_subscriptions(self) -> None:
        headers = [
            "subscription_id", "user_id", "plan_name", "price_monthly",
            "max_streams", "max_resolution", "max_profiles", "status",
            "start_date", "end_date", "auto_renew", "created_at",
        ]
        sid = 0

        def rows():
            nonlocal sid
            plan_names = list(SUBSCRIPTION_PLANS.keys())
            for uid in range(1, self.config.users + 1):
                n_subs = random.choices([1, 2, 3], weights=[60, 30, 10])[0]
                for i in range(n_subs):
                    sid += 1
                    plan_name = random.choices(plan_names, weights=PLAN_WEIGHTS)[0]
                    plan = SUBSCRIPTION_PLANS[plan_name]
                    is_current = i == n_subs - 1
                    status = "active" if is_current else random.choice(SUB_STATUSES_PREV)
                    start = _random_date(date(2020, 1, 1), date(2025, 6, 1))
                    end = "" if status == "active" else str(start + timedelta(days=random.randint(30, 365)))
                    auto_renew = _bool(status == "active" or random.random() < 0.3)
                    created = _fmt_ts(datetime.combine(start, datetime.min.time()))
                    yield [
                        sid, uid, plan_name, plan["price"],
                        plan["max_streams"], plan["resolution"], plan["max_profiles"],
                        status, start, end, auto_renew, created,
                    ]

        self._write_csv("subscriptions.csv", headers, rows())
        self._subscription_count = sid

    def _generate_payments(self) -> None:
        headers = [
            "payment_id", "subscription_id", "amount", "currency",
            "payment_method", "transaction_id", "status", "paid_at", "created_at",
        ]
        pmid = 0

        def rows():
            nonlocal pmid
            plan_prices = [p["price"] for p in SUBSCRIPTION_PLANS.values()]
            for sub_id in range(1, self._subscription_count + 1):
                n_payments = random.randint(1, 6)
                for _ in range(n_payments):
                    pmid += 1
                    amount = random.choice(plan_prices)
                    method = random.choice(PAYMENT_METHODS)
                    tx_id = str(uuid.uuid4())
                    status = random.choice(PAYMENT_STATUSES)
                    created = _random_datetime(PLATFORM_START, PLATFORM_END)
                    if status == "completed":
                        paid_at = _fmt_ts(created + timedelta(seconds=random.randint(0, 3600)))
                    else:
                        paid_at = ""
                    yield [
                        pmid, sub_id, amount, "PLN",
                        method, tx_id, status, paid_at, _fmt_ts(created),
                    ]

        self._write_csv("payments.csv", headers, rows())

    def _generate_people(self) -> None:
        n = self.config.people
        headers = [
            "person_id", "first_name", "last_name",
            "birth_date", "bio", "photo_url", "nationality",
        ]

        def rows():
            for pid in range(1, n + 1):
                first = random.choice(self._first_names)
                last = random.choice(self._last_names)
                birth = _random_date(date(1940, 1, 1), date(2000, 1, 1))
                bio = random.choice(self._bios) if random.random() < 0.7 else ""
                photo = f"https://photo.example.com/person/{pid}.jpg" if random.random() < 0.8 else ""
                nat = random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0]
                yield [pid, first, last, birth, bio, photo, nat]

        self._write_csv("people.csv", headers, rows())

    def _generate_content(self) -> None:
        n = self.config.content
        headers = [
            "content_id", "title", "description", "type", "release_date",
            "duration_minutes", "maturity_rating", "genres",
            "poster_url", "trailer_url", "avg_rating", "total_views",
            "popularity_score", "country_of_origin", "original_language",
            "is_active", "created_at",
        ]

        def rows():
            for cid in range(1, n + 1):
                is_series = random.random() < 0.4
                content_type = "series" if is_series else "movie"
                (self._series_ids if is_series else self._movie_ids).append(cid)

                title = random.choice(self._titles)
                desc = random.choice(self._descriptions)
                rel_date = _random_date(date(1990, 1, 1), date(2025, 12, 1))
                duration = "" if is_series else random.randint(75, 210)
                maturity = random.choice(MATURITY_RATINGS)
                genres = ",".join(random.sample(GENRES, random.randint(1, 3)))
                poster = f"https://cdn.example.com/posters/{cid}.jpg"
                trailer = f"https://cdn.example.com/trailers/{cid}.mp4" if random.random() < 0.8 else ""
                avg_rating = round(random.uniform(1.0, 9.5), 2) if random.random() < 0.85 else 0.00
                total_views = random.randint(100, 10_000_000)
                popularity = round(random.uniform(0, 100), 2)
                country = random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0]
                lang = random.choices(LANGUAGES, weights=[30, 40, 10, 10, 10])[0]
                is_active = _bool(random.random() < 0.95)
                created = _random_datetime(PLATFORM_START, PLATFORM_END)
                yield [
                    cid, title, desc, content_type, rel_date, duration,
                    maturity, genres, poster, trailer, avg_rating, total_views,
                    popularity, country, lang, is_active, _fmt_ts(created),
                ]

        self._write_csv("content.csv", headers, rows())

    def _generate_content_people(self) -> None:
        headers = ["content_id", "person_id", "role", "character_name", "billing_order"]
        n_people = self.config.people
        all_content_ids = self._movie_ids + self._series_ids

        def rows():
            for cid in all_content_ids:
                used: set[int] = set()

                director = random.randint(1, n_people)
                used.add(director)
                yield [cid, director, "director", "", ""]

                for pid in _sample_ids(n_people, used, random.randint(1, 2)):
                    used.add(pid)
                    yield [cid, pid, "writer", "", ""]

                actors = _sample_ids(n_people, used, random.randint(3, 12))
                for order, pid in enumerate(actors, 1):
                    char = f"{random.choice(self._first_names)} {random.choice(self._last_names)}"
                    yield [cid, pid, "actor", char, order]

        self._write_csv("content_people.csv", headers, rows())

    def _generate_seasons(self) -> None:
        headers = ["season_id", "content_id", "season_number", "title", "release_date"]
        sid = 0

        def rows():
            nonlocal sid
            for cid in self._series_ids:
                n_seasons = random.choices(range(1, 9), weights=SEASON_COUNT_WEIGHTS)[0]
                base = _random_date(date(2015, 1, 1), date(2024, 1, 1))
                for sn in range(1, n_seasons + 1):
                    sid += 1
                    self._season_map.append((sid, cid))
                    rel = base + timedelta(days=365 * (sn - 1) + random.randint(0, 60))
                    yield [sid, cid, sn, f"Sezon {sn}", rel]

        self._write_csv("seasons.csv", headers, rows())

    def _generate_episodes(self) -> None:
        headers = [
            "episode_id", "season_id", "episode_number", "title",
            "description", "duration_minutes", "release_date", "video_url",
        ]
        eid = 0

        def rows():
            nonlocal eid
            for season_id, content_id in self._season_map:
                n_eps = random.randint(4, 16)
                base = _random_date(date(2015, 1, 1), date(2025, 6, 1))
                for en in range(1, n_eps + 1):
                    eid += 1
                    self._content_episode_map.setdefault(content_id, []).append(eid)
                    title = random.choice(self._ep_titles)
                    desc = random.choice(self._descriptions) if random.random() < 0.5 else ""
                    duration = random.randint(20, 65)
                    rel = base + timedelta(days=7 * (en - 1))
                    video = f"https://cdn.example.com/videos/{eid}.mp4"
                    yield [eid, season_id, en, title, desc, duration, rel, video]

        self._write_csv("episodes.csv", headers, rows())

    def _generate_watch_history(self) -> None:
        n = self.config.watch_history
        headers = [
            "watch_id", "profile_id", "content_id", "episode_id",
            "started_at", "progress_percent", "completed",
        ]
        all_content = self._movie_ids + self._series_ids

        def rows():
            for wid in range(1, n + 1):
                pid = random.randint(1, self._profile_count)
                cid = random.choice(all_content)
                episodes = self._content_episode_map.get(cid)
                episode_id = random.choice(episodes) if episodes else ""
                started = _random_datetime(PLATFORM_START, PLATFORM_END)
                progress = round(random.uniform(0, 100), 2)
                completed = progress > 95
                if completed:
                    progress = 100.00
                yield [
                    wid, pid, cid, episode_id,
                    _fmt_ts(started), progress, _bool(completed),
                ]

        self._write_csv("watch_history.csv", headers, rows())

    def _generate_my_list(self) -> None:
        headers = ["list_id", "profile_id", "content_id", "added_at", "sort_order"]
        all_content = self._movie_ids + self._series_ids
        lo, hi = self.config.my_list_per_profile
        lid = 0

        def rows():
            nonlocal lid
            max_items = min(hi, len(all_content))
            for pid in range(1, self._profile_count + 1):
                n_items = random.randint(lo, max_items)
                if n_items == 0:
                    continue
                items = random.sample(all_content, n_items)
                for order, cid in enumerate(items, 1):
                    lid += 1
                    added = _random_datetime(PLATFORM_START, PLATFORM_END)
                    yield [lid, pid, cid, _fmt_ts(added), order]

        self._write_csv("my_list.csv", headers, rows())

    def _generate_ratings(self) -> None:
        headers = [
            "rating_id", "profile_id", "content_id", "score",
            "review_text", "created_at", "updated_at",
        ]
        all_content = self._movie_ids + self._series_ids
        lo, hi = self.config.ratings_per_profile
        rid = 0

        def rows():
            nonlocal rid
            max_items = min(hi, len(all_content))
            for pid in range(1, self._profile_count + 1):
                n_ratings = random.randint(lo, max_items)
                if n_ratings == 0:
                    continue
                rated = random.sample(all_content, n_ratings)
                for cid in rated:
                    rid += 1
                    score = random.choices(range(1, 11), weights=SCORE_WEIGHTS)[0]
                    review = random.choice(self._reviews) if random.random() < 0.3 else ""
                    created = _random_datetime(PLATFORM_START, PLATFORM_END)
                    updated = _random_datetime(created, PLATFORM_END)
                    yield [
                        rid, pid, cid, score,
                        review, _fmt_ts(created), _fmt_ts(updated),
                    ]

        self._write_csv("ratings.csv", headers, rows())

    def _write_csv(self, filename: str, headers: list[str], rows) -> int:
        filepath = self.output_dir / filename
        count = 0
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for row in rows:
                writer.writerow(row)
                count += 1
        return count


def _random_date(start: date, end: date) -> date:
    return start + timedelta(days=random.randint(0, (end - start).days))


def _random_datetime(start: datetime, end: datetime) -> datetime:
    return start + timedelta(seconds=random.uniform(0, (end - start).total_seconds()))


def _fmt_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _bool(value: bool) -> str:
    return "true" if value else "false"


def _sample_ids(max_id: int, exclude: set[int], k: int) -> list[int]:
    selected: list[int] = []
    seen = set(exclude)
    while len(selected) < k:
        pid = random.randint(1, max_id)
        if pid not in seen:
            seen.add(pid)
            selected.append(pid)
    return selected
