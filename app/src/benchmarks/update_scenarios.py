import random

from pymongo import UpdateOne

from .base import BaseScenario, BenchmarkContext


class U1_UpdateWatchProgress(BaseScenario):
    id = "U1"
    name = "Update watch progress"
    category = "UPDATE"

    def setup_postgres(self, conn, ctx):
        self._wid = ctx.random_id("watch_history")
        row = conn.execute(
            "SELECT progress_percent, completed FROM watch_history WHERE watch_id = %s",
            (self._wid,),
        ).fetchone()
        self._orig_progress = row[0] if row else 50
        self._orig_completed = row[1] if row else False

    def setup_mysql(self, conn, ctx):
        self._wid = ctx.random_id("watch_history")
        cur = conn.cursor()
        cur.execute(
            "SELECT progress_percent, completed FROM watch_history WHERE watch_id = %s",
            (self._wid,),
        )
        row = cur.fetchone()
        self._orig_progress = row[0] if row else 50
        self._orig_completed = row[1] if row else False

    def setup_mongo(self, db, ctx):
        self._wid = ctx.random_id("watch_history")

    def setup_neo4j(self, driver, ctx):
        self._pid = ctx.random_id("profiles")

    def run_postgres(self, conn, ctx):
        conn.execute("""
            UPDATE watch_history
            SET progress_percent = 75.50, completed = FALSE
            WHERE watch_id = %s
        """, (self._wid,))
        conn.commit()

    def run_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute("""
            UPDATE watch_history
            SET progress_percent = 75.50, completed = FALSE
            WHERE watch_id = %s
        """, (self._wid,))
        conn.commit()

    def run_mongo(self, db, ctx):
        db.watch_history.update_one(
            {"_id": self._wid},
            {"$set": {"progress_percent": 75.50, "completed": False}},
        )

    def run_neo4j(self, driver, ctx):
        with driver.session() as s:
            s.run("""
                MATCH (p:Profile {profile_id: $pid})-[w:WATCHED]->()
                WITH w LIMIT 1
                SET w.progress_percent = 75.50, w.completed = false
            """, pid=self._pid).consume()

    def teardown_postgres(self, conn, ctx):
        conn.execute("""
            UPDATE watch_history
            SET progress_percent = %s, completed = %s
            WHERE watch_id = %s
        """, (self._orig_progress, self._orig_completed, self._wid))
        conn.commit()

    def teardown_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute("""
            UPDATE watch_history
            SET progress_percent = %s, completed = %s
            WHERE watch_id = %s
        """, (self._orig_progress, self._orig_completed, self._wid))
        conn.commit()


class U2_RecalcAvgRating(BaseScenario):
    id = "U2"
    name = "Recalculate avg_rating for content"
    category = "UPDATE"

    def setup_postgres(self, conn, ctx):
        self._cid = ctx.random_id("content")

    setup_mysql = setup_postgres
    setup_mongo = setup_postgres
    setup_neo4j = setup_postgres

    def run_postgres(self, conn, ctx):
        conn.execute("""
            UPDATE content SET avg_rating = COALESCE(
                (SELECT AVG(score)::DECIMAL(3,2) FROM ratings
                 WHERE content_id = %s), 0)
            WHERE content_id = %s
        """, (self._cid, self._cid))
        conn.commit()

    def run_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute("""
            UPDATE content SET avg_rating = COALESCE(
                (SELECT AVG(score) FROM ratings
                 WHERE content_id = %s), 0)
            WHERE content_id = %s
        """, (self._cid, self._cid))
        conn.commit()

    def run_mongo(self, db, ctx):
        pipeline = [
            {"$match": {"content_id": self._cid}},
            {"$group": {"_id": None, "avg": {"$avg": "$score"}}},
        ]
        result = list(db.ratings.aggregate(pipeline))
        avg = result[0]["avg"] if result else 0
        db.content.update_one({"_id": self._cid}, {"$set": {"avg_rating": avg}})

    def run_neo4j(self, driver, ctx):
        with driver.session() as s:
            s.run("""
                MATCH (:Profile)-[r:RATED]->(c:Content {content_id: $cid})
                WITH c, avg(r.score) AS avgScore
                SET c.avg_rating = avgScore
            """, cid=self._cid).consume()


class U3_MassSubPlanChange(BaseScenario):
    id = "U3"
    name = "Mass subscription plan change (Basic->Standard)"
    category = "UPDATE"

    def run_postgres(self, conn, ctx):
        conn.execute("""
            UPDATE subscriptions
            SET plan_name = 'Standard', price_monthly = 43.99,
                max_streams = 2, max_resolution = '1080p'
            WHERE status = 'active' AND plan_name = 'Basic'
        """)
        conn.commit()

    def run_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute("""
            UPDATE subscriptions
            SET plan_name = 'Standard', price_monthly = 43.99,
                max_streams = 2, max_resolution = '1080p'
            WHERE status = 'active' AND plan_name = 'Basic'
        """)
        conn.commit()

    def run_mongo(self, db, ctx):
        db.users.update_many(
            {"subscription.status": "active",
             "subscription.plan_name": "Basic"},
            {"$set": {
                "subscription.plan_name": "Standard",
                "subscription.price_monthly": 43.99,
                "subscription.max_streams": 2,
                "subscription.max_resolution": "1080p",
            }},
        )

    def run_neo4j(self, driver, ctx):
        with driver.session() as s:
            s.run("""
                MATCH (s:Subscription)
                WHERE s.status = 'active' AND s.plan_name = 'Basic'
                SET s.plan_name = 'Standard', s.price_monthly = 43.99,
                    s.max_streams = 2, s.max_resolution = '1080p'
            """).consume()

    def teardown_postgres(self, conn, ctx):
        conn.execute("""
            UPDATE subscriptions
            SET plan_name = 'Basic', price_monthly = 29.99,
                max_streams = 1, max_resolution = '720p'
            WHERE status = 'active' AND plan_name = 'Standard'
              AND price_monthly = 43.99 AND max_streams = 2
        """)
        conn.commit()

    def teardown_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute("""
            UPDATE subscriptions
            SET plan_name = 'Basic', price_monthly = 29.99,
                max_streams = 1, max_resolution = '720p'
            WHERE status = 'active' AND plan_name = 'Standard'
              AND price_monthly = 43.99 AND max_streams = 2
        """)
        conn.commit()

    def teardown_mongo(self, db, ctx):
        db.users.update_many(
            {"subscription.status": "active",
             "subscription.plan_name": "Standard",
             "subscription.price_monthly": 43.99},
            {"$set": {
                "subscription.plan_name": "Basic",
                "subscription.price_monthly": 29.99,
                "subscription.max_streams": 1,
                "subscription.max_resolution": "720p",
            }},
        )

    def teardown_neo4j(self, driver, ctx):
        with driver.session() as s:
            s.run("""
                MATCH (s:Subscription)
                WHERE s.status = 'active' AND s.plan_name = 'Standard'
                  AND s.price_monthly = 43.99
                SET s.plan_name = 'Basic', s.price_monthly = 29.99,
                    s.max_streams = 1, s.max_resolution = '720p'
            """).consume()


class U4_UpdateUserData(BaseScenario):
    id = "U4"
    name = "Update user data (email, phone)"
    category = "UPDATE"

    def setup_postgres(self, conn, ctx):
        self._uid = ctx.random_id("users")
        row = conn.execute(
            "SELECT email, phone FROM users WHERE user_id = %s", (self._uid,)
        ).fetchone()
        self._orig_email = row[0] if row else ""
        self._orig_phone = row[1] if row else ""

    def setup_mysql(self, conn, ctx):
        self._uid = ctx.random_id("users")
        cur = conn.cursor()
        cur.execute("SELECT email, phone FROM users WHERE user_id = %s", (self._uid,))
        row = cur.fetchone()
        self._orig_email = row[0] if row else ""
        self._orig_phone = row[1] if row else ""

    def setup_mongo(self, db, ctx):
        self._uid = ctx.random_id("users")
        doc = db.users.find_one({"_id": self._uid}, {"email": 1, "phone": 1})
        self._orig_email = doc["email"] if doc else ""
        self._orig_phone = doc.get("phone", "") if doc else ""

    def setup_neo4j(self, driver, ctx):
        self._uid = ctx.random_id("users")

    def run_postgres(self, conn, ctx):
        conn.execute("""
            UPDATE users
            SET email = %s, phone = '+48 111 222 333', updated_at = NOW()
            WHERE user_id = %s
        """, (f"updated_{self._uid}@bench.com", self._uid))
        conn.commit()

    def run_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute("""
            UPDATE users
            SET email = %s, phone = '+48 111 222 333', updated_at = NOW()
            WHERE user_id = %s
        """, (f"updated_{self._uid}@bench.com", self._uid))
        conn.commit()

    def run_mongo(self, db, ctx):
        db.users.update_one(
            {"_id": self._uid},
            {"$set": {
                "email": f"updated_{self._uid}@bench.com",
                "phone": "+48 111 222 333",
                "updated_at": "2025-06-15 12:00:00",
            }},
        )

    def run_neo4j(self, driver, ctx):
        with driver.session() as s:
            s.run("""
                MATCH (u:User {user_id: $uid})
                SET u.email = $email
            """, uid=self._uid,
                email=f"updated_{self._uid}@bench.com").consume()

    def teardown_postgres(self, conn, ctx):
        conn.execute(
            "UPDATE users SET email = %s, phone = %s WHERE user_id = %s",
            (self._orig_email, self._orig_phone, self._uid),
        )
        conn.commit()

    def teardown_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute(
            "UPDATE users SET email = %s, phone = %s WHERE user_id = %s",
            (self._orig_email, self._orig_phone, self._uid),
        )
        conn.commit()

    def teardown_mongo(self, db, ctx):
        db.users.update_one(
            {"_id": self._uid},
            {"$set": {"email": self._orig_email, "phone": self._orig_phone}},
        )

    def teardown_neo4j(self, driver, ctx):
        pass


class U5_MarkContentInactive(BaseScenario):
    id = "U5"
    name = "Mark content as inactive"
    category = "UPDATE"

    def setup_postgres(self, conn, ctx):
        self._cid = ctx.random_id("content")

    setup_mysql = setup_postgres
    setup_mongo = setup_postgres
    setup_neo4j = setup_postgres

    def run_postgres(self, conn, ctx):
        conn.execute(
            "UPDATE content SET is_active = FALSE WHERE content_id = %s",
            (self._cid,),
        )
        conn.commit()

    def run_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute(
            "UPDATE content SET is_active = FALSE WHERE content_id = %s",
            (self._cid,),
        )
        conn.commit()

    def run_mongo(self, db, ctx):
        db.content.update_one(
            {"_id": self._cid}, {"$set": {"is_active": False}}
        )

    def run_neo4j(self, driver, ctx):
        with driver.session() as s:
            s.run("""
                MATCH (c:Content {content_id: $cid})
                SET c.is_active = false
            """, cid=self._cid).consume()

    def teardown_postgres(self, conn, ctx):
        conn.execute(
            "UPDATE content SET is_active = TRUE WHERE content_id = %s",
            (self._cid,),
        )
        conn.commit()

    def teardown_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute(
            "UPDATE content SET is_active = TRUE WHERE content_id = %s",
            (self._cid,),
        )
        conn.commit()

    def teardown_mongo(self, db, ctx):
        db.content.update_one(
            {"_id": self._cid}, {"$set": {"is_active": True}}
        )

    def teardown_neo4j(self, driver, ctx):
        with driver.session() as s:
            s.run("""
                MATCH (c:Content {content_id: $cid})
                SET c.is_active = true
            """, cid=self._cid).consume()


class U6_MassPopularityUpdate(BaseScenario):
    id = "U6"
    name = "Mass update popularity_score"
    category = "UPDATE"

    def run_postgres(self, conn, ctx):
        conn.execute("""
            UPDATE content SET popularity_score = (
                total_views * 0.0001 +
                avg_rating * 5 +
                COALESCE((SELECT COUNT(*) FROM ratings
                          WHERE content_id = content.content_id), 0) * 0.1
            )
        """)
        conn.commit()

    def run_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute("""
            UPDATE content c SET popularity_score = (
                c.total_views * 0.0001 +
                c.avg_rating * 5 +
                COALESCE((SELECT COUNT(*) FROM ratings r
                          WHERE r.content_id = c.content_id), 0) * 0.1
            )
        """)
        conn.commit()

    def run_mongo(self, db, ctx):
        pipeline = [
            {"$group": {"_id": "$content_id", "cnt": {"$sum": 1}}},
        ]
        ratings_counts = {
            r["_id"]: r["cnt"] for r in db.ratings.aggregate(pipeline)
        }
        ops = []
        for doc in db.content.find({}, {"total_views": 1, "avg_rating": 1}):
            cid = doc["_id"]
            score = (
                doc.get("total_views", 0) * 0.0001
                + doc.get("avg_rating", 0) * 5
                + ratings_counts.get(cid, 0) * 0.1
            )
            ops.append(UpdateOne(
                {"_id": cid}, {"$set": {"popularity_score": round(score, 2)}}
            ))
        if ops:
            db.content.bulk_write(ops, ordered=False)

    def run_neo4j(self, driver, ctx):
        with driver.session() as s:
            s.run("""
                MATCH (c:Content)
                OPTIONAL MATCH (:Profile)-[r:RATED]->(c)
                WITH c, c.total_views * 0.0001 + c.avg_rating * 5 +
                     count(r) * 0.1 AS score
                SET c.popularity_score = score
            """).consume()


UPDATE_SCENARIOS = [
    U1_UpdateWatchProgress(),
    U2_RecalcAvgRating(),
    U3_MassSubPlanChange(),
    U4_UpdateUserData(),
    U5_MarkContentInactive(),
    U6_MassPopularityUpdate(),
]
