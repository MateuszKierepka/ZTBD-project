import random
import uuid
from io import StringIO

from pymongo import UpdateOne

from .base import BaseScenario, BenchmarkContext


class I1_RegisterUser(BaseScenario):
    id = "I1"
    name = "Register user (multi-table)"
    category = "INSERT"

    def setup_postgres(self, conn, ctx):
        self._email = f"bench_{uuid.uuid4().hex[:8]}@test.com"

    setup_mysql = setup_postgres
    setup_mongo = setup_postgres
    setup_neo4j = setup_postgres

    def run_postgres(self, conn, ctx):
        row = conn.execute("""
            INSERT INTO users (email, password_hash, first_name, last_name,
                date_of_birth, country_code, status, created_at, updated_at)
            VALUES (%s, '$2b$12$benchmark', 'Bench', 'User',
                '1990-01-01', 'PL', 'active', NOW(), NOW())
            RETURNING user_id
        """, (self._email,)).fetchone()
        uid = row[0]
        conn.execute("""
            INSERT INTO profiles (user_id, name, maturity_rating, language, created_at)
            VALUES (%s, 'Main', 'ALL', 'pl', NOW())
        """, (uid,))
        conn.execute("""
            INSERT INTO subscriptions (user_id, plan_name, price_monthly, max_streams,
                max_resolution, status, start_date, auto_renew, created_at)
            VALUES (%s, 'Standard', 43.99, 2, '1080p', 'active', CURRENT_DATE, TRUE, NOW())
        """, (uid,))
        conn.commit()
        self._uid = uid

    def run_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO users (email, password_hash, first_name, last_name,
                date_of_birth, country_code, status, created_at, updated_at)
            VALUES (%s, '$2b$12$benchmark', 'Bench', 'User',
                '1990-01-01', 'PL', 'active', NOW(), NOW())
        """, (self._email,))
        uid = cur.lastrowid
        cur.execute("""
            INSERT INTO profiles (user_id, name, maturity_rating, language, created_at)
            VALUES (%s, 'Main', 'ALL', 'pl', NOW())
        """, (uid,))
        cur.execute("""
            INSERT INTO subscriptions (user_id, plan_name, price_monthly, max_streams,
                max_resolution, status, start_date, auto_renew, created_at)
            VALUES (%s, 'Standard', 43.99, 2, '1080p', 'active', CURDATE(), TRUE, NOW())
        """, (uid,))
        conn.commit()
        self._uid = uid

    def run_mongo(self, db, ctx):
        uid = ctx.test_id("users")
        db.users.insert_one({
            "_id": uid,
            "email": self._email,
            "password_hash": "$2b$12$benchmark",
            "first_name": "Bench", "last_name": "User",
            "date_of_birth": "1990-01-01", "country_code": "PL",
            "status": "active", "created_at": "2025-01-01 00:00:00",
            "updated_at": "2025-01-01 00:00:00",
            "profiles": [{
                "profile_id": uid * 10,
                "name": "Main", "is_kids": False,
                "maturity_rating": "ALL", "language": "pl",
                "created_at": "2025-01-01 00:00:00",
            }],
            "subscription": {
                "subscription_id": uid * 10,
                "plan_name": "Standard", "price_monthly": 43.99,
                "max_streams": 2, "max_resolution": "1080p",
                "status": "active", "start_date": "2025-01-01",
                "auto_renew": True,
            },
        })
        self._uid = uid

    def run_neo4j(self, driver, ctx):
        uid = ctx.test_id("users")
        with driver.session() as s:
            s.run("""
                CREATE (u:User {user_id: $uid, email: $email,
                    first_name: 'Bench', last_name: 'User',
                    country_code: 'PL', status: 'active',
                    created_at: '2025-01-01 00:00:00'})
                CREATE (p:Profile {profile_id: $pid, name: 'Main',
                    is_kids: false, maturity_rating: 'ALL', language: 'pl'})
                CREATE (sub:Subscription {subscription_id: $sid,
                    plan_name: 'Standard', price_monthly: 43.99,
                    max_streams: 2, max_resolution: '1080p',
                    status: 'active', start_date: '2025-01-01',
                    auto_renew: true})
                CREATE (u)-[:HAS_PROFILE]->(p)
                CREATE (u)-[:HAS_SUBSCRIPTION]->(sub)
            """, uid=uid, email=self._email,
                pid=uid * 10, sid=uid * 10).consume()
        self._uid = uid

    def teardown_postgres(self, conn, ctx):
        conn.execute("DELETE FROM users WHERE user_id = %s", (self._uid,))
        conn.commit()

    def teardown_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute("DELETE FROM users WHERE user_id = %s", (self._uid,))
        conn.commit()

    def teardown_mongo(self, db, ctx):
        db.users.delete_one({"_id": self._uid})

    def teardown_neo4j(self, driver, ctx):
        with driver.session() as s:
            s.run("""
                MATCH (u:User {user_id: $uid})
                OPTIONAL MATCH (u)-[r]->()
                DELETE r, u
            """, uid=self._uid).consume()
            s.run("MATCH (p:Profile {profile_id: $pid}) DETACH DELETE p",
                  pid=self._uid * 10).consume()
            s.run("MATCH (s:Subscription {subscription_id: $sid}) DETACH DELETE s",
                  sid=self._uid * 10).consume()


class I2_BulkWatchHistory(BaseScenario):
    id = "I2"
    name = "Bulk import watch_history"
    category = "INSERT"

    _BENCH_DATE = "2037-12-31 00:00:00"

    def setup_postgres(self, conn, ctx):
        n = ctx.params["batch_watch_history"]
        max_pid = ctx.max_ids["profiles"]
        max_cid = ctx.max_ids["content"]
        self._start_wid = ctx.max_ids["watch_history"] + 100_000
        self._n = n
        self._data = []
        for i in range(n):
            self._data.append((
                random.randint(1, max_pid),
                random.randint(1, max_cid),
                "\\N",
                self._BENCH_DATE,
                round(random.uniform(0, 100), 2),
                str(random.random() > 0.5).lower(),
            ))

    setup_mysql = setup_postgres
    setup_mongo = setup_postgres
    setup_neo4j = setup_postgres

    def run_postgres(self, conn, ctx):
        buf = StringIO()
        for row in self._data:
            buf.write("\t".join(str(v) for v in row) + "\n")
        buf.seek(0)
        with conn.cursor() as cur:
            with cur.copy(
                "COPY watch_history (profile_id, content_id, episode_id, "
                "started_at, progress_percent, completed) FROM STDIN"
            ) as copy:
                for line in buf:
                    copy.write(line)
        conn.commit()

    def run_mysql(self, conn, ctx):
        cur = conn.cursor()
        rows = [(d[0], d[1], d[3], d[4], 1 if d[5] == "true" else 0)
                for d in self._data]
        batch_size = 5000
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            placeholders = ", ".join(["(%s, %s, NULL, %s, %s, %s)"] * len(batch))
            flat = [v for row in batch for v in row]
            cur.execute(
                f"INSERT INTO watch_history (profile_id, content_id, episode_id, "
                f"started_at, progress_percent, completed) VALUES {placeholders}",
                flat,
            )
        conn.commit()

    def run_mongo(self, db, ctx):
        base_id = self._start_wid
        docs = [
            {
                "_id": base_id + i,
                "profile_id": d[0],
                "content_id": d[1],
                "episode_id": None,
                "started_at": d[3],
                "progress_percent": d[4],
                "completed": d[5] == "true",
            }
            for i, d in enumerate(self._data)
        ]
        for j in range(0, len(docs), 5000):
            db.watch_history.insert_many(docs[j:j + 5000], ordered=False)

    def run_neo4j(self, driver, ctx):
        rows = [
            {
                "profile_id": d[0], "content_id": d[1],
                "started_at": d[3], "progress_percent": d[4],
                "completed": d[5] == "true",
            }
            for d in self._data
        ]
        with driver.session() as s:
            for j in range(0, len(rows), 5000):
                batch = rows[j:j + 5000]
                s.run("""
                    UNWIND $rows AS r
                    MATCH (p:Profile {profile_id: r.profile_id})
                    MATCH (c:Content {content_id: r.content_id})
                    CREATE (p)-[:WATCHED {started_at: r.started_at,
                        progress_percent: r.progress_percent,
                        completed: r.completed}]->(c)
                """, rows=batch).consume()

    def teardown_postgres(self, conn, ctx):
        conn.execute(
            "DELETE FROM watch_history WHERE watch_id > %s",
            (ctx.max_ids["watch_history"],),
        )
        conn.commit()

    def teardown_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM watch_history WHERE watch_id > %s",
            (ctx.max_ids["watch_history"],),
        )
        conn.commit()

    def teardown_mongo(self, db, ctx):
        db.watch_history.delete_many({"_id": {"$gte": self._start_wid}})

    def teardown_neo4j(self, driver, ctx):
        with driver.session() as s:
            s.run("""
                MATCH (p:Profile)-[w:WATCHED]->(c:Content)
                WHERE w.started_at = $date
                DELETE w
            """, date=self._BENCH_DATE).consume()


class I3_AddSeriesWithTree(BaseScenario):
    id = "I3"
    name = "Add series with full tree"
    category = "INSERT"

    def setup_postgres(self, conn, ctx):
        self._email = f"bench_{uuid.uuid4().hex[:8]}@test.com"

    setup_mysql = setup_postgres
    setup_mongo = setup_postgres
    setup_neo4j = setup_postgres

    def run_postgres(self, conn, ctx):
        row = conn.execute("""
            INSERT INTO content (title, description, type, release_date,
                maturity_rating, genres, avg_rating, popularity_score,
                country_of_origin, original_language, is_active,
                metadata, created_at)
            VALUES ('Benchmark Series', 'Test', 'series', '2025-01-01',
                '16+', 'Drama,Thriller', 0, 0, 'PL', 'pl', TRUE,
                '{"studio":"Test","budget":1000000,"awards":[],"tags":["test"],"production_countries":["PL"],"streaming_quality":{"max_resolution":"4K","hdr_supported":false,"dolby_atmos":false}}'::jsonb,
                NOW())
            RETURNING content_id
        """).fetchone()
        cid = row[0]
        for sn in range(1, 4):
            srow = conn.execute("""
                INSERT INTO seasons (content_id, season_number, title, release_date)
                VALUES (%s, %s, %s, '2025-01-01') RETURNING season_id
            """, (cid, sn, f"Season {sn}")).fetchone()
            sid = srow[0]
            for en in range(1, 11):
                conn.execute("""
                    INSERT INTO episodes (season_id, episode_number, title,
                        description, duration_minutes, release_date, video_url)
                    VALUES (%s, %s, %s, 'Test episode', 45, '2025-01-01',
                        'https://cdn.example.com/test.mp4')
                """, (sid, en, f"Episode {en}"))
        max_people = ctx.max_ids["people"]
        for i in range(1, 11):
            pid = random.randint(1, max_people)
            role = "actor" if i > 2 else ("director" if i == 1 else "writer")
            conn.execute("""
                INSERT INTO content_people (content_id, person_id, role,
                    character_name, billing_order)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (cid, pid, role,
                  f"Character {i}" if role == "actor" else None,
                  i if role == "actor" else None))
        conn.commit()
        self._cid = cid

    def run_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO content (title, description, type, release_date,
                maturity_rating, genres, avg_rating, popularity_score,
                country_of_origin, original_language, is_active,
                metadata, created_at)
            VALUES ('Benchmark Series', 'Test', 'series', '2025-01-01',
                '16+', 'Drama,Thriller', 0, 0, 'PL', 'pl', TRUE,
                '{"studio":"Test","budget":1000000,"awards":[],"tags":["test"],"production_countries":["PL"],"streaming_quality":{"max_resolution":"4K","hdr_supported":false,"dolby_atmos":false}}',
                NOW())
        """)
        cid = cur.lastrowid
        for sn in range(1, 4):
            cur.execute("""
                INSERT INTO seasons (content_id, season_number, title, release_date)
                VALUES (%s, %s, %s, '2025-01-01')
            """, (cid, sn, f"Season {sn}"))
            sid = cur.lastrowid
            for en in range(1, 11):
                cur.execute("""
                    INSERT INTO episodes (season_id, episode_number, title,
                        description, duration_minutes, release_date, video_url)
                    VALUES (%s, %s, %s, 'Test episode', 45, '2025-01-01',
                        'https://cdn.example.com/test.mp4')
                """, (sid, en, f"Episode {en}"))
        max_people = ctx.max_ids["people"]
        for i in range(1, 11):
            pid = random.randint(1, max_people)
            role = "actor" if i > 2 else ("director" if i == 1 else "writer")
            cur.execute("""
                INSERT IGNORE INTO content_people (content_id, person_id, role,
                    character_name, billing_order)
                VALUES (%s, %s, %s, %s, %s)
            """, (cid, pid, role,
                  f"Character {i}" if role == "actor" else None,
                  i if role == "actor" else None))
        conn.commit()
        self._cid = cid

    def run_mongo(self, db, ctx):
        import json
        cid = ctx.test_id("content")
        seasons = []
        for sn in range(1, 4):
            episodes = [
                {"episode_id": cid * 1000 + sn * 100 + en,
                 "episode_number": en, "title": f"Episode {en}",
                 "duration_minutes": 45, "release_date": "2025-01-01"}
                for en in range(1, 11)
            ]
            seasons.append({
                "season_id": cid * 100 + sn, "season_number": sn,
                "title": f"Season {sn}", "release_date": "2025-01-01",
                "episodes": episodes,
            })
        cast = [
            {"person_id": random.randint(1, ctx.max_ids["people"]),
             "first_name": "Test", "last_name": f"Actor{i}",
             "role": "actor", "character_name": f"Character {i}",
             "billing_order": i}
            for i in range(1, 11)
        ]
        db.content.insert_one({
            "_id": cid, "title": "Benchmark Series",
            "description": "Test", "type": "series",
            "release_date": "2025-01-01", "maturity_rating": "16+",
            "genres": ["Drama", "Thriller"], "avg_rating": 0,
            "total_views": 0, "popularity_score": 0,
            "is_active": True,
            "metadata": {"studio": "Test", "budget": 1000000, "awards": [],
                         "tags": ["test"], "production_countries": ["PL"],
                         "streaming_quality": {"max_resolution": "4K",
                                               "hdr_supported": False,
                                               "dolby_atmos": False}},
            "created_at": "2025-01-01 00:00:00",
            "cast": cast, "seasons": seasons,
        })
        self._cid = cid

    def run_neo4j(self, driver, ctx):
        cid = ctx.test_id("content")
        metadata = '{"studio":"Test","budget":1000000,"awards":[],"tags":["test"]}'
        with driver.session() as s:
            s.run("""
                CREATE (c:Content {content_id: $cid, title: 'Benchmark Series',
                    type: 'series', maturity_rating: '16+',
                    avg_rating: 0, popularity_score: 0,
                    is_active: true, metadata: $meta,
                    created_at: '2025-01-01 00:00:00'})
                WITH c
                UNWIND range(1, 3) AS sn
                CREATE (s:Season {season_id: $cid * 100 + sn,
                    season_number: sn, title: 'Season ' + toString(sn)})
                CREATE (c)-[:HAS_SEASON]->(s)
                WITH s, sn
                UNWIND range(1, 10) AS en
                CREATE (e:Episode {episode_id: $cid * 1000 + sn * 100 + en,
                    episode_number: en, title: 'Episode ' + toString(en),
                    duration_minutes: 45})
                CREATE (s)-[:HAS_EPISODE]->(e)
            """, cid=cid, meta=metadata).consume()
            people_ids = [random.randint(1, ctx.max_ids["people"])
                          for _ in range(10)]
            s.run("""
                UNWIND $pids AS pid
                MATCH (p:Person {person_id: pid})
                MATCH (c:Content {content_id: $cid})
                CREATE (p)-[:ACTED_IN {character_name: 'Test',
                    billing_order: pid % 10}]->(c)
            """, pids=people_ids, cid=cid).consume()
        self._cid = cid

    def teardown_postgres(self, conn, ctx):
        conn.execute("DELETE FROM content WHERE content_id = %s", (self._cid,))
        conn.commit()

    def teardown_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute("DELETE FROM content WHERE content_id = %s", (self._cid,))
        conn.commit()

    def teardown_mongo(self, db, ctx):
        db.content.delete_one({"_id": self._cid})

    def teardown_neo4j(self, driver, ctx):
        with driver.session() as s:
            s.run("""
                MATCH (c:Content {content_id: $cid})
                OPTIONAL MATCH (c)-[:HAS_SEASON]->(s:Season)-[:HAS_EPISODE]->(e:Episode)
                DETACH DELETE e, s
            """, cid=self._cid).consume()
            s.run("""
                MATCH (c:Content {content_id: $cid})
                DETACH DELETE c
            """, cid=self._cid).consume()


class I4_BatchPayments(BaseScenario):
    id = "I4"
    name = "Batch insert payments"
    category = "INSERT"

    def setup_postgres(self, conn, ctx):
        n = ctx.params["batch_payments"]
        max_sid = ctx.max_ids["subscriptions"]
        self._n = n
        self._start_id = ctx.max_ids["payments"] + 100_000
        methods = ["card", "blik", "paypal", "transfer"]
        amounts = [29.99, 43.99, 59.99]
        self._data = [
            (random.randint(1, max_sid), random.choice(amounts), "PLN",
             random.choice(methods), str(uuid.uuid4()), "completed",
             "2025-06-15 12:00:00", "2025-06-15 12:00:00")
            for _ in range(n)
        ]

    setup_mysql = setup_postgres
    setup_mongo = setup_postgres
    setup_neo4j = setup_postgres

    def run_postgres(self, conn, ctx):
        buf = StringIO()
        for d in self._data:
            buf.write("\t".join(str(v) for v in d) + "\n")
        buf.seek(0)
        with conn.cursor() as cur:
            with cur.copy(
                "COPY payments (subscription_id, amount, currency, "
                "payment_method, transaction_id, status, paid_at, created_at) "
                "FROM STDIN"
            ) as copy:
                for line in buf:
                    copy.write(line)
        conn.commit()

    def run_mysql(self, conn, ctx):
        cur = conn.cursor()
        batch_size = 5000
        for i in range(0, len(self._data), batch_size):
            batch = self._data[i:i + batch_size]
            placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, %s, %s)"] * len(batch))
            flat = [v for row in batch for v in row]
            cur.execute(
                f"INSERT INTO payments (subscription_id, amount, currency, "
                f"payment_method, transaction_id, status, paid_at, created_at) "
                f"VALUES {placeholders}",
                flat,
            )
        conn.commit()

    def run_mongo(self, db, ctx):
        docs = [
            {
                "_id": self._start_id + i,
                "subscription_id": d[0], "amount": d[1], "currency": d[2],
                "payment_method": d[3], "transaction_id": d[4],
                "status": d[5], "paid_at": d[6], "created_at": d[7],
            }
            for i, d in enumerate(self._data)
        ]
        for j in range(0, len(docs), 5000):
            db.payments.insert_many(docs[j:j + 5000], ordered=False)

    def run_neo4j(self, driver, ctx):
        rows = [
            {
                "payment_id": self._start_id + i,
                "subscription_id": d[0], "amount": d[1], "currency": d[2],
                "payment_method": d[3], "status": d[5], "paid_at": d[6],
            }
            for i, d in enumerate(self._data)
        ]
        with driver.session() as s:
            for j in range(0, len(rows), 5000):
                batch = rows[j:j + 5000]
                s.run("""
                    UNWIND $rows AS r
                    MATCH (sub:Subscription {subscription_id: r.subscription_id})
                    CREATE (pay:Payment {payment_id: r.payment_id,
                        amount: r.amount, currency: r.currency,
                        payment_method: r.payment_method,
                        status: r.status, paid_at: r.paid_at})
                    CREATE (sub)-[:HAS_PAYMENT]->(pay)
                """, rows=batch).consume()

    def teardown_postgres(self, conn, ctx):
        conn.execute(
            "DELETE FROM payments WHERE payment_id > %s",
            (ctx.max_ids["payments"],),
        )
        conn.commit()

    def teardown_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM payments WHERE payment_id > %s",
            (ctx.max_ids["payments"],),
        )
        conn.commit()

    def teardown_mongo(self, db, ctx):
        db.payments.delete_many({"_id": {"$gte": self._start_id}})

    def teardown_neo4j(self, driver, ctx):
        with driver.session() as s:
            s.run("""
                MATCH (pay:Payment)
                WHERE pay.payment_id >= $start
                DETACH DELETE pay
            """, start=self._start_id).consume()


class I5_RatingsWithAvgUpdate(BaseScenario):
    id = "I5"
    name = "Add ratings with avg_rating recalc"
    category = "INSERT"

    def setup_postgres(self, conn, ctx):
        n = ctx.params["batch_ratings"]
        self._cid = ctx.random_id("content")
        max_pid = ctx.max_ids["profiles"]
        self._n = n
        self._start_id = ctx.max_ids["ratings"] + 100_000
        self._data = [
            (random.randint(1, max_pid), self._cid,
             random.randint(1, 10), "", "2025-06-15 12:00:00",
             "2025-06-15 12:00:00")
            for _ in range(n)
        ]

    setup_mysql = setup_postgres
    setup_mongo = setup_postgres
    setup_neo4j = setup_postgres

    def run_postgres(self, conn, ctx):
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO ratings (profile_id, content_id, score,
                    review_text, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (profile_id, content_id) DO NOTHING
            """, self._data)
        conn.execute("""
            UPDATE content SET avg_rating = COALESCE(
                (SELECT AVG(score) FROM ratings WHERE content_id = %s), 0)
            WHERE content_id = %s
        """, (self._cid, self._cid))
        conn.commit()

    def run_mysql(self, conn, ctx):
        cur = conn.cursor()
        batch_size = 5000
        for i in range(0, len(self._data), batch_size):
            batch = self._data[i:i + batch_size]
            placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s)"] * len(batch))
            flat = [v for row in batch for v in row]
            cur.execute(
                f"INSERT IGNORE INTO ratings (profile_id, content_id, score, "
                f"review_text, created_at, updated_at) VALUES {placeholders}",
                flat,
            )
        cur.execute("""
            UPDATE content SET avg_rating = COALESCE(
                (SELECT AVG(score) FROM ratings WHERE content_id = %s), 0)
            WHERE content_id = %s
        """, (self._cid, self._cid))
        conn.commit()

    def run_mongo(self, db, ctx):
        docs = [
            {
                "_id": self._start_id + i,
                "profile_id": d[0], "content_id": d[1],
                "score": d[2], "review_text": None,
                "created_at": d[4], "updated_at": d[5],
            }
            for i, d in enumerate(self._data)
        ]
        for j in range(0, len(docs), 5000):
            try:
                db.ratings.insert_many(docs[j:j + 5000], ordered=False)
            except Exception:
                pass
        pipeline = [
            {"$match": {"content_id": self._cid}},
            {"$group": {"_id": None, "avg": {"$avg": "$score"}}},
        ]
        result = list(db.ratings.aggregate(pipeline))
        avg = result[0]["avg"] if result else 0
        db.content.update_one({"_id": self._cid}, {"$set": {"avg_rating": avg}})

    def run_neo4j(self, driver, ctx):
        rows = [
            {"profile_id": d[0], "content_id": d[1], "score": d[2]}
            for d in self._data
        ]
        with driver.session() as s:
            for j in range(0, len(rows), 5000):
                batch = rows[j:j + 5000]
                s.run("""
                    UNWIND $rows AS r
                    MATCH (p:Profile {profile_id: r.profile_id})
                    MATCH (c:Content {content_id: r.content_id})
                    CREATE (p)-[:RATED {score: r.score,
                        created_at: '2025-06-15 12:00:00'}]->(c)
                """, rows=batch).consume()
            s.run("""
                MATCH (:Profile)-[r:RATED]->(c:Content {content_id: $cid})
                WITH c, avg(r.score) AS avgScore
                SET c.avg_rating = avgScore
            """, cid=self._cid).consume()

    def teardown_postgres(self, conn, ctx):
        conn.execute(
            "DELETE FROM ratings WHERE rating_id > %s",
            (ctx.max_ids["ratings"],),
        )
        conn.commit()

    def teardown_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM ratings WHERE rating_id > %s",
            (ctx.max_ids["ratings"],),
        )
        conn.commit()

    def teardown_mongo(self, db, ctx):
        db.ratings.delete_many({"_id": {"$gte": self._start_id}})

    def teardown_neo4j(self, driver, ctx):
        with driver.session() as s:
            s.run("""
                MATCH (p:Profile)-[r:RATED]->(c:Content {content_id: $cid})
                DELETE r
            """, cid=self._cid).consume()


class I6_ImportPeopleWithRelations(BaseScenario):
    id = "I6"
    name = "Import people with content associations"
    category = "INSERT"

    def setup_postgres(self, conn, ctx):
        n = ctx.params["batch_people"]
        self._n = n
        self._start_id = ctx.max_ids["people"] + 100_000
        max_cid = ctx.max_ids["content"]
        self._people = [
            (self._start_id + i, f"BenchFirst{i}", f"BenchLast{i}",
             "1985-05-15", "Test bio", "\\N", "PL")
            for i in range(n)
        ]
        self._relations = []
        for i in range(n):
            pid = self._start_id + i
            cid = random.randint(1, max_cid)
            self._relations.append((cid, pid, "actor", f"Character {i}", i + 1))

    setup_mysql = setup_postgres
    setup_mongo = setup_postgres
    setup_neo4j = setup_postgres

    def run_postgres(self, conn, ctx):
        buf = StringIO()
        for p in self._people:
            buf.write("\t".join(str(v) if v is not None else "" for v in p) + "\n")
        buf.seek(0)
        with conn.cursor() as cur:
            with cur.copy(
                "COPY people (person_id, first_name, last_name, birth_date, "
                "bio, photo_url, nationality) FROM STDIN"
            ) as copy:
                for line in buf:
                    copy.write(line)
        for r in self._relations:
            conn.execute("""
                INSERT INTO content_people (content_id, person_id, role,
                    character_name, billing_order)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, r)
        conn.commit()

    def run_mysql(self, conn, ctx):
        cur = conn.cursor()
        batch_size = 5000
        for i in range(0, len(self._people), batch_size):
            batch = self._people[i:i + batch_size]
            placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, %s)"] * len(batch))
            flat = [v for row in batch for v in row]
            cur.execute(
                f"INSERT INTO people (person_id, first_name, last_name, "
                f"birth_date, bio, photo_url, nationality) "
                f"VALUES {placeholders}",
                flat,
            )
        for i in range(0, len(self._relations), batch_size):
            batch = self._relations[i:i + batch_size]
            placeholders = ", ".join(["(%s, %s, %s, %s, %s)"] * len(batch))
            flat = [v for row in batch for v in row]
            cur.execute(
                f"INSERT IGNORE INTO content_people (content_id, person_id, role, "
                f"character_name, billing_order) VALUES {placeholders}",
                flat,
            )
        conn.commit()

    def run_mongo(self, db, ctx):
        ops = [
            UpdateOne(
                {"_id": r[0]},
                {"$push": {"cast": {
                    "person_id": r[1],
                    "first_name": f"BenchFirst{r[1] - self._start_id}",
                    "last_name": f"BenchLast{r[1] - self._start_id}",
                    "role": r[2], "character_name": r[3],
                    "billing_order": r[4],
                }}},
            )
            for r in self._relations
        ]
        if ops:
            db.content.bulk_write(ops, ordered=False)

    def run_neo4j(self, driver, ctx):
        with driver.session() as s:
            rows = [
                {"person_id": p[0], "first_name": p[1], "last_name": p[2],
                 "birth_date": p[3], "nationality": p[6]}
                for p in self._people
            ]
            for j in range(0, len(rows), 5000):
                s.run("""
                    UNWIND $rows AS r
                    CREATE (:Person {person_id: r.person_id,
                        first_name: r.first_name, last_name: r.last_name,
                        birth_date: r.birth_date, nationality: r.nationality})
                """, rows=rows[j:j + 5000]).consume()
            rels = [
                {"content_id": r[0], "person_id": r[1],
                 "character_name": r[3], "billing_order": r[4]}
                for r in self._relations
            ]
            for j in range(0, len(rels), 5000):
                s.run("""
                    UNWIND $rows AS r
                    MATCH (p:Person {person_id: r.person_id})
                    MATCH (c:Content {content_id: r.content_id})
                    CREATE (p)-[:ACTED_IN {character_name: r.character_name,
                        billing_order: r.billing_order}]->(c)
                """, rows=rels[j:j + 5000]).consume()

    def teardown_postgres(self, conn, ctx):
        conn.execute(
            "DELETE FROM people WHERE person_id >= %s", (self._start_id,)
        )
        conn.commit()

    def teardown_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM people WHERE person_id >= %s", (self._start_id,)
        )
        conn.commit()

    def teardown_mongo(self, db, ctx):
        ops = [
            UpdateOne(
                {"_id": r[0]},
                {"$pull": {"cast": {"person_id": r[1]}}},
            )
            for r in self._relations
        ]
        if ops:
            db.content.bulk_write(ops, ordered=False)

    def teardown_neo4j(self, driver, ctx):
        with driver.session() as s:
            s.run("""
                MATCH (p:Person)
                WHERE p.person_id >= $start
                DETACH DELETE p
            """, start=self._start_id).consume()


INSERT_SCENARIOS = [
    I1_RegisterUser(),
    I2_BulkWatchHistory(),
    I3_AddSeriesWithTree(),
    I4_BatchPayments(),
    I5_RatingsWithAvgUpdate(),
    I6_ImportPeopleWithRelations(),
]
