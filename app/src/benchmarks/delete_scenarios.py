import random
import uuid

from .base import BaseScenario, BenchmarkContext


class D1_DeleteContentCascade(BaseScenario):
    id = "D1"
    name = "Delete content with cascade"
    category = "DELETE"

    def setup_postgres(self, conn, ctx):
        row = conn.execute("""
            INSERT INTO content (title, type, maturity_rating, genres,
                is_active, metadata, created_at)
            VALUES ('_del_test', 'movie', 'ALL', 'Drama', TRUE,
                '{"studio":"Test","budget":0,"awards":[],"tags":[],"production_countries":[],"streaming_quality":{"max_resolution":"HD","hdr_supported":false,"dolby_atmos":false}}'::jsonb,
                NOW())
            RETURNING content_id
        """).fetchone()
        cid = row[0]
        for i in range(10):
            pid = random.randint(1, ctx.max_ids["profiles"])
            conn.execute("""
                INSERT INTO watch_history (profile_id, content_id, started_at,
                    progress_percent, completed)
                VALUES (%s, %s, NOW(), 50, FALSE)
            """, (pid, cid))
            conn.execute("""
                INSERT INTO my_list (profile_id, content_id, added_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT DO NOTHING
            """, (pid, cid))
        conn.commit()
        self._cid = cid

    def setup_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO content (title, type, maturity_rating, genres,
                is_active, metadata, created_at)
            VALUES ('_del_test', 'movie', 'ALL', 'Drama', TRUE,
                '{"studio":"Test","budget":0,"awards":[],"tags":[],"production_countries":[],"streaming_quality":{"max_resolution":"HD","hdr_supported":false,"dolby_atmos":false}}',
                NOW())
        """)
        cid = cur.lastrowid
        for i in range(10):
            pid = random.randint(1, ctx.max_ids["profiles"])
            cur.execute("""
                INSERT INTO watch_history (profile_id, content_id, started_at,
                    progress_percent, completed)
                VALUES (%s, %s, NOW(), 50, FALSE)
            """, (pid, cid))
            cur.execute("""
                INSERT IGNORE INTO my_list (profile_id, content_id, added_at)
                VALUES (%s, %s, NOW())
            """, (pid, cid))
        conn.commit()
        self._cid = cid

    def setup_mongo(self, db, ctx):
        cid = ctx.test_id("content")
        db.content.insert_one({
            "_id": cid, "title": "_del_test", "type": "movie",
            "genres": ["Drama"], "is_active": True,
            "metadata": {"studio": "Test", "budget": 0, "awards": [],
                         "tags": [], "production_countries": [],
                         "streaming_quality": {"max_resolution": "HD",
                                               "hdr_supported": False,
                                               "dolby_atmos": False}},
            "cast": [], "seasons": [],
        })
        wh_docs = []
        ml_docs = []
        base_wid = ctx.test_id("watch_history") + 500_000
        base_lid = ctx.test_id("my_list") + 500_000
        for i in range(10):
            pid = random.randint(1, ctx.max_ids["profiles"])
            wh_docs.append({
                "_id": base_wid + i, "profile_id": pid, "content_id": cid,
                "started_at": "2025-06-15 12:00:00",
                "progress_percent": 50, "completed": False,
            })
            ml_docs.append({
                "_id": base_lid + i, "profile_id": pid, "content_id": cid,
                "added_at": "2025-06-15 12:00:00", "sort_order": 0,
            })
        db.watch_history.insert_many(wh_docs, ordered=False)
        try:
            db.my_list.insert_many(ml_docs, ordered=False)
        except Exception:
            pass
        self._cid = cid

    def setup_neo4j(self, driver, ctx):
        cid = ctx.test_id("content")
        with driver.session() as s:
            s.run("""
                CREATE (c:Content {content_id: $cid, title: '_del_test',
                    type: 'movie', is_active: true,
                    created_at: '2025-01-01 00:00:00'})
            """, cid=cid).consume()
            s.run("""
                MATCH (p:Profile) WITH p ORDER BY rand() LIMIT 10
                MATCH (c:Content {content_id: $cid})
                CREATE (p)-[:WATCHED {started_at: '2025-06-15',
                    progress_percent: 50, completed: false}]->(c)
            """, cid=cid).consume()
        self._cid = cid

    def run_postgres(self, conn, ctx):
        conn.execute(
            "DELETE FROM content WHERE content_id = %s", (self._cid,)
        )
        conn.commit()

    def run_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM content WHERE content_id = %s", (self._cid,)
        )
        conn.commit()

    def run_mongo(self, db, ctx):
        db.watch_history.delete_many({"content_id": self._cid})
        db.my_list.delete_many({"content_id": self._cid})
        db.ratings.delete_many({"content_id": self._cid})
        db.content.delete_one({"_id": self._cid})

    def run_neo4j(self, driver, ctx):
        with driver.session() as s:
            s.run("""
                MATCH (c:Content {content_id: $cid})
                OPTIONAL MATCH (c)-[:HAS_SEASON]->(s:Season)
                              -[:HAS_EPISODE]->(e:Episode)
                DETACH DELETE e, s
            """, cid=self._cid).consume()
            s.run("""
                MATCH (c:Content {content_id: $cid})
                DETACH DELETE c
            """, cid=self._cid).consume()


class D2_DeleteProfileWithHistory(BaseScenario):
    id = "D2"
    name = "Delete profile with history (cascade)"
    category = "DELETE"

    def setup_postgres(self, conn, ctx):
        uid = random.randint(1, ctx.max_ids["users"])
        row = conn.execute("""
            INSERT INTO profiles (user_id, name, maturity_rating, language,
                created_at)
            VALUES (%s, '_del_profile', 'ALL', 'pl', NOW())
            RETURNING profile_id
        """, (uid,)).fetchone()
        pid = row[0]
        for i in range(20):
            cid = random.randint(1, ctx.max_ids["content"])
            conn.execute("""
                INSERT INTO watch_history (profile_id, content_id, started_at,
                    progress_percent, completed)
                VALUES (%s, %s, NOW(), 50, FALSE)
            """, (pid, cid))
        conn.commit()
        self._pid = pid

    def setup_mysql(self, conn, ctx):
        cur = conn.cursor()
        uid = random.randint(1, ctx.max_ids["users"])
        cur.execute("""
            INSERT INTO profiles (user_id, name, maturity_rating, language,
                created_at)
            VALUES (%s, '_del_profile', 'ALL', 'pl', NOW())
        """, (uid,))
        pid = cur.lastrowid
        for i in range(20):
            cid = random.randint(1, ctx.max_ids["content"])
            cur.execute("""
                INSERT INTO watch_history (profile_id, content_id, started_at,
                    progress_percent, completed)
                VALUES (%s, %s, NOW(), 50, FALSE)
            """, (pid, cid))
        conn.commit()
        self._pid = pid

    def setup_mongo(self, db, ctx):
        pid = ctx.test_id("profiles") + 500_000
        uid = random.randint(1, ctx.max_ids["users"])
        db.users.update_one(
            {"_id": uid},
            {"$push": {"profiles": {
                "profile_id": pid, "name": "_del_profile",
                "is_kids": False, "maturity_rating": "ALL",
                "language": "pl",
            }}},
        )
        base_wid = ctx.test_id("watch_history") + 600_000
        wh_docs = [
            {
                "_id": base_wid + i, "profile_id": pid,
                "content_id": random.randint(1, ctx.max_ids["content"]),
                "started_at": "2025-06-15 12:00:00",
                "progress_percent": 50, "completed": False,
            }
            for i in range(20)
        ]
        db.watch_history.insert_many(wh_docs, ordered=False)
        self._pid = pid
        self._uid = uid

    def setup_neo4j(self, driver, ctx):
        pid = ctx.test_id("profiles") + 500_000
        uid = random.randint(1, ctx.max_ids["users"])
        with driver.session() as s:
            s.run("""
                MATCH (u:User {user_id: $uid})
                CREATE (p:Profile {profile_id: $pid, name: '_del_profile',
                    is_kids: false, maturity_rating: 'ALL', language: 'pl'})
                CREATE (u)-[:HAS_PROFILE]->(p)
            """, uid=uid, pid=pid).consume()
            s.run("""
                MATCH (c:Content) WITH c ORDER BY rand() LIMIT 20
                MATCH (p:Profile {profile_id: $pid})
                CREATE (p)-[:WATCHED {started_at: '2025-06-15',
                    progress_percent: 50, completed: false}]->(c)
            """, pid=pid).consume()
        self._pid = pid

    def run_postgres(self, conn, ctx):
        conn.execute(
            "DELETE FROM profiles WHERE profile_id = %s", (self._pid,)
        )
        conn.commit()

    def run_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM profiles WHERE profile_id = %s", (self._pid,)
        )
        conn.commit()

    def run_mongo(self, db, ctx):
        db.watch_history.delete_many({"profile_id": self._pid})
        db.my_list.delete_many({"profile_id": self._pid})
        db.ratings.delete_many({"profile_id": self._pid})
        db.users.update_one(
            {"_id": self._uid},
            {"$pull": {"profiles": {"profile_id": self._pid}}},
        )

    def run_neo4j(self, driver, ctx):
        with driver.session() as s:
            s.run("""
                MATCH (p:Profile {profile_id: $pid})
                DETACH DELETE p
            """, pid=self._pid).consume()


class D3_CleanOldHistory(BaseScenario):
    id = "D3"
    name = "Clean old ratings"
    category = "DELETE"

    _TEST_DATE = "2000-01-15 12:00:00"
    _TEST_CUTOFF = "2000-06-01"

    def setup_postgres(self, conn, ctx):
        n = ctx.params["batch_watch_history"]
        conn.execute("""
            INSERT INTO ratings
                (profile_id, content_id, score, review_text, created_at, updated_at)
            SELECT
                (floor(random() * %s) + 1)::BIGINT,
                (floor(random() * %s) + 1)::BIGINT,
                (floor(random() * 10) + 1)::INT,
                '', %s::TIMESTAMP, %s::TIMESTAMP
            FROM generate_series(1, %s)
            ON CONFLICT (profile_id, content_id) DO NOTHING
        """, (ctx.max_ids["profiles"], ctx.max_ids["content"],
              self._TEST_DATE, self._TEST_DATE, n))
        conn.commit()

    def setup_mysql(self, conn, ctx):
        n = ctx.params["batch_watch_history"]
        cur = conn.cursor()
        rows = [
            (random.randint(1, ctx.max_ids["profiles"]),
             random.randint(1, ctx.max_ids["content"]),
             random.randint(1, 10), "", self._TEST_DATE, self._TEST_DATE)
            for _ in range(n)
        ]
        batch_size = 10_000
        for i in range(0, n, batch_size):
            batch = rows[i:i + batch_size]
            placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s)"] * len(batch))
            flat = [v for row in batch for v in row]
            cur.execute(
                "INSERT IGNORE INTO ratings (profile_id, content_id, score, "
                "review_text, created_at, updated_at) "
                f"VALUES {placeholders}",
                flat,
            )
            conn.commit()

    def setup_mongo(self, db, ctx):
        n = ctx.params["batch_watch_history"]
        base_rid = ctx.test_id("ratings") + 400_000
        for i in range(0, n, 10_000):
            batch_size = min(10_000, n - i)
            docs = [
                {
                    "_id": base_rid + i + j,
                    "profile_id": random.randint(1, ctx.max_ids["profiles"]),
                    "content_id": random.randint(1, ctx.max_ids["content"]),
                    "score": random.randint(1, 10),
                    "review_text": "",
                    "created_at": self._TEST_DATE,
                    "updated_at": self._TEST_DATE,
                }
                for j in range(batch_size)
            ]
            try:
                db.ratings.insert_many(docs, ordered=False)
            except Exception:
                pass

    def setup_neo4j(self, driver, ctx):
        n = ctx.params["batch_watch_history"]
        rows = [
            {"pid": random.randint(1, ctx.max_ids["profiles"]),
             "cid": random.randint(1, ctx.max_ids["content"]),
             "score": round(random.uniform(1, 10), 1)}
            for _ in range(n)
        ]
        with driver.session() as s:
            for j in range(0, n, 5000):
                s.run("""
                    UNWIND $rows AS r
                    MATCH (p:Profile {profile_id: r.pid})
                    MATCH (c:Content {content_id: r.cid})
                    CREATE (p)-[:RATED {score: r.score, rated_at: $date}]->(c)
                """, rows=rows[j:j + 5000], date=self._TEST_DATE).consume()

    def run_postgres(self, conn, ctx):
        conn.execute(
            "DELETE FROM ratings WHERE created_at < %s",
            (self._TEST_CUTOFF,),
        )
        conn.commit()

    def run_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM ratings WHERE created_at < %s",
            (self._TEST_CUTOFF,),
        )
        conn.commit()

    def run_mongo(self, db, ctx):
        db.ratings.delete_many({"created_at": {"$lt": self._TEST_CUTOFF}})

    def run_neo4j(self, driver, ctx):
        with driver.session() as s:
            while True:
                result = s.run("""
                    MATCH ()-[r:RATED]->()
                    WHERE r.rated_at < $cutoff
                    WITH r LIMIT 10000
                    DELETE r
                    RETURN count(*) AS deleted
                """, cutoff=self._TEST_CUTOFF)
                deleted = result.single()["deleted"]
                if deleted == 0:
                    break


class D4_RemoveFromMyList(BaseScenario):
    id = "D4"
    name = "Remove from my_list"
    category = "DELETE"

    def setup_postgres(self, conn, ctx):
        pid = random.randint(1, ctx.max_ids["profiles"])
        cid = random.randint(1, ctx.max_ids["content"])
        conn.execute("""
            INSERT INTO my_list (profile_id, content_id, added_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (profile_id, content_id) DO NOTHING
        """, (pid, cid))
        conn.commit()
        self._pid = pid
        self._cid = cid

    def setup_mysql(self, conn, ctx):
        pid = random.randint(1, ctx.max_ids["profiles"])
        cid = random.randint(1, ctx.max_ids["content"])
        cur = conn.cursor()
        cur.execute("""
            INSERT IGNORE INTO my_list (profile_id, content_id, added_at)
            VALUES (%s, %s, NOW())
        """, (pid, cid))
        conn.commit()
        self._pid = pid
        self._cid = cid

    def setup_mongo(self, db, ctx):
        pid = random.randint(1, ctx.max_ids["profiles"])
        cid = random.randint(1, ctx.max_ids["content"])
        lid = ctx.test_id("my_list") + 700_000
        try:
            db.my_list.insert_one({
                "_id": lid, "profile_id": pid, "content_id": cid,
                "added_at": "2025-06-15 12:00:00", "sort_order": 0,
            })
        except Exception:
            pass
        self._pid = pid
        self._cid = cid

    def setup_neo4j(self, driver, ctx):
        pid = random.randint(1, ctx.max_ids["profiles"])
        cid = random.randint(1, ctx.max_ids["content"])
        with driver.session() as s:
            s.run("""
                MATCH (p:Profile {profile_id: $pid})
                MATCH (c:Content {content_id: $cid})
                CREATE (p)-[:ADDED_TO_LIST {added_at: '2025-06-15'}]->(c)
            """, pid=pid, cid=cid).consume()
        self._pid = pid
        self._cid = cid

    def run_postgres(self, conn, ctx):
        conn.execute("""
            DELETE FROM my_list
            WHERE profile_id = %s AND content_id = %s
        """, (self._pid, self._cid))
        conn.commit()

    def run_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM my_list
            WHERE profile_id = %s AND content_id = %s
        """, (self._pid, self._cid))
        conn.commit()

    def run_mongo(self, db, ctx):
        db.my_list.delete_one(
            {"profile_id": self._pid, "content_id": self._cid}
        )

    def run_neo4j(self, driver, ctx):
        with driver.session() as s:
            s.run("""
                MATCH (p:Profile {profile_id: $pid})
                      -[r:ADDED_TO_LIST]->
                      (c:Content {content_id: $cid})
                DELETE r
            """, pid=self._pid, cid=self._cid).consume()


class D5_DeleteSubscriptionCascade(BaseScenario):
    id = "D5"
    name = "Delete subscription with payments"
    category = "DELETE"

    def setup_postgres(self, conn, ctx):
        uid = random.randint(1, ctx.max_ids["users"])
        row = conn.execute("""
            INSERT INTO subscriptions (user_id, plan_name, price_monthly,
                max_streams, max_resolution, status, start_date, auto_renew,
                created_at)
            VALUES (%s, 'Basic', 29.99, 1, '720p', 'cancelled',
                '2020-01-01', FALSE, NOW())
            RETURNING subscription_id
        """, (uid,)).fetchone()
        sid = row[0]
        for _ in range(5):
            conn.execute("""
                INSERT INTO payments (subscription_id, amount, currency,
                    payment_method, transaction_id, status, paid_at, created_at)
                VALUES (%s, 29.99, 'PLN', 'card', %s, 'completed', NOW(), NOW())
            """, (sid, str(uuid.uuid4())))
        conn.commit()
        self._sid = sid

    def setup_mysql(self, conn, ctx):
        cur = conn.cursor()
        uid = random.randint(1, ctx.max_ids["users"])
        cur.execute("""
            INSERT INTO subscriptions (user_id, plan_name, price_monthly,
                max_streams, max_resolution, status, start_date, auto_renew,
                created_at)
            VALUES (%s, 'Basic', 29.99, 1, '720p', 'cancelled',
                '2020-01-01', FALSE, NOW())
        """, (uid,))
        sid = cur.lastrowid
        for _ in range(5):
            cur.execute("""
                INSERT INTO payments (subscription_id, amount, currency,
                    payment_method, transaction_id, status, paid_at, created_at)
                VALUES (%s, 29.99, 'PLN', 'card', %s, 'completed', NOW(), NOW())
            """, (sid, str(uuid.uuid4())))
        conn.commit()
        self._sid = sid

    def setup_mongo(self, db, ctx):
        sid = ctx.test_id("subscriptions") + 500_000
        base_pid = ctx.test_id("payments") + 800_000
        docs = [
            {
                "_id": base_pid + i,
                "subscription_id": sid, "amount": 29.99, "currency": "PLN",
                "payment_method": "card", "transaction_id": str(uuid.uuid4()),
                "status": "completed", "paid_at": "2025-06-15 12:00:00",
                "created_at": "2025-06-15 12:00:00",
            }
            for i in range(5)
        ]
        db.payments.insert_many(docs, ordered=False)
        self._sid = sid

    def setup_neo4j(self, driver, ctx):
        sid = ctx.test_id("subscriptions") + 500_000
        uid = random.randint(1, ctx.max_ids["users"])
        with driver.session() as s:
            s.run("""
                MATCH (u:User {user_id: $uid})
                CREATE (sub:Subscription {subscription_id: $sid,
                    plan_name: 'Basic', price_monthly: 29.99,
                    status: 'cancelled', start_date: '2020-01-01'})
                CREATE (u)-[:HAS_SUBSCRIPTION]->(sub)
                WITH sub
                UNWIND range(1, 5) AS i
                CREATE (pay:Payment {payment_id: $sid * 100 + i,
                    amount: 29.99, currency: 'PLN',
                    payment_method: 'card', status: 'completed'})
                CREATE (sub)-[:HAS_PAYMENT]->(pay)
            """, uid=uid, sid=sid).consume()
        self._sid = sid

    def run_postgres(self, conn, ctx):
        conn.execute(
            "DELETE FROM subscriptions WHERE subscription_id = %s",
            (self._sid,),
        )
        conn.commit()

    def run_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM subscriptions WHERE subscription_id = %s",
            (self._sid,),
        )
        conn.commit()

    def run_mongo(self, db, ctx):
        db.payments.delete_many({"subscription_id": self._sid})

    def run_neo4j(self, driver, ctx):
        with driver.session() as s:
            s.run("""
                MATCH (sub:Subscription {subscription_id: $sid})
                OPTIONAL MATCH (sub)-[:HAS_PAYMENT]->(pay:Payment)
                DETACH DELETE pay, sub
            """, sid=self._sid).consume()


class D6_MassDeleteInactiveUsers(BaseScenario):
    id = "D6"
    name = "Mass delete inactive users"
    category = "DELETE"

    def setup_postgres(self, conn, ctx):
        n = ctx.params["batch_users_delete"]
        self._start_uid = ctx.max_ids["users"] + 200_000
        for i in range(n):
            uid = self._start_uid + i
            conn.execute("""
                INSERT INTO users (user_id, email, password_hash, first_name,
                    last_name, date_of_birth, country_code, status,
                    created_at, updated_at)
                VALUES (%s, %s, '$2b$12$del', 'Del', 'User', '1990-01-01',
                    'PL', 'deleted', NOW(), NOW())
            """, (uid, f"del_{uid}@test.com"))
        conn.commit()
        self._n = n

    def setup_mysql(self, conn, ctx):
        n = ctx.params["batch_users_delete"]
        self._start_uid = ctx.max_ids["users"] + 200_000
        cur = conn.cursor()
        for i in range(n):
            uid = self._start_uid + i
            cur.execute("""
                INSERT INTO users (user_id, email, password_hash, first_name,
                    last_name, date_of_birth, country_code, status,
                    created_at, updated_at)
                VALUES (%s, %s, '$2b$12$del', 'Del', 'User', '1990-01-01',
                    'PL', 'deleted', NOW(), NOW())
            """, (uid, f"del_{uid}@test.com"))
        conn.commit()
        self._n = n

    def setup_mongo(self, db, ctx):
        n = ctx.params["batch_users_delete"]
        self._start_uid = ctx.max_ids["users"] + 200_000
        docs = [
            {
                "_id": self._start_uid + i,
                "email": f"del_{self._start_uid + i}@test.com",
                "password_hash": "$2b$12$del",
                "first_name": "Del", "last_name": "User",
                "date_of_birth": "1990-01-01", "country_code": "PL",
                "status": "deleted",
                "created_at": "2025-01-01 00:00:00",
                "updated_at": "2025-01-01 00:00:00",
                "profiles": [], "subscription": None,
            }
            for i in range(n)
        ]
        db.users.insert_many(docs, ordered=False)
        self._n = n

    def setup_neo4j(self, driver, ctx):
        n = ctx.params["batch_users_delete"]
        self._start_uid = ctx.max_ids["users"] + 200_000
        rows = [
            {"user_id": self._start_uid + i,
             "email": f"del_{self._start_uid + i}@test.com"}
            for i in range(n)
        ]
        with driver.session() as s:
            for j in range(0, len(rows), 5000):
                s.run("""
                    UNWIND $rows AS r
                    CREATE (:User {user_id: r.user_id, email: r.email,
                        first_name: 'Del', last_name: 'User',
                        country_code: 'PL', status: 'deleted',
                        created_at: '2025-01-01 00:00:00'})
                """, rows=rows[j:j + 5000]).consume()
        self._n = n

    def run_postgres(self, conn, ctx):
        conn.execute(
            "DELETE FROM users WHERE status = 'deleted' AND user_id >= %s AND user_id < %s",
            (self._start_uid, self._start_uid + self._n),
        )
        conn.commit()

    def run_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM users WHERE status = 'deleted' AND user_id >= %s AND user_id < %s",
            (self._start_uid, self._start_uid + self._n),
        )
        conn.commit()

    def run_mongo(self, db, ctx):
        db.users.delete_many({
            "status": "deleted",
            "_id": {"$gte": self._start_uid,
                    "$lt": self._start_uid + self._n},
        })

    def run_neo4j(self, driver, ctx):
        with driver.session() as s:
            s.run("""
                MATCH (u:User)
                WHERE u.status = 'deleted' AND u.user_id >= $start AND u.user_id < $end
                DETACH DELETE u
            """, start=self._start_uid,
                end=self._start_uid + self._n).consume()


DELETE_SCENARIOS = [
    D1_DeleteContentCascade(),
    D2_DeleteProfileWithHistory(),
    D3_CleanOldHistory(),
    D4_RemoveFromMyList(),
    D5_DeleteSubscriptionCascade(),
    D6_MassDeleteInactiveUsers(),
]
