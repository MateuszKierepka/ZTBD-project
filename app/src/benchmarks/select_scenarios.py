from .base import BaseScenario, BenchmarkContext


class S1_Homepage(BaseScenario):
    id = "S1"
    name = "Homepage: filter+sort by popularity"
    category = "SELECT"

    def run_postgres(self, conn, ctx):
        conn.execute("""
            SELECT content_id, title, genres, popularity_score, avg_rating,
                   metadata->>'studio' AS studio
            FROM content
            WHERE is_active = TRUE AND genres LIKE '%%Action%%'
            ORDER BY popularity_score DESC
            LIMIT 20
        """).fetchall()

    def run_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute("""
            SELECT content_id, title, genres, popularity_score, avg_rating,
                   metadata->>'$.studio' AS studio
            FROM content
            WHERE is_active = TRUE AND genres LIKE '%%Action%%'
            ORDER BY popularity_score DESC
            LIMIT 20
        """)
        cur.fetchall()

    def run_mongo(self, db, ctx):
        list(db.content.find(
            {"is_active": True, "genres": "Action"},
            {"title": 1, "genres": 1, "popularity_score": 1,
             "avg_rating": 1, "metadata.studio": 1},
        ).sort("popularity_score", -1).limit(20))

    def run_neo4j(self, driver, ctx):
        with driver.session() as s:
            s.run("""
                MATCH (c:Content)-[:HAS_GENRE]->(g:Genre {name: 'Action'})
                WHERE c.is_active = true
                RETURN c.content_id, c.title, c.popularity_score, c.avg_rating
                ORDER BY c.popularity_score DESC
                LIMIT 20
            """).consume()


class S2_CollaborativeFiltering(BaseScenario):
    id = "S2"
    name = "Collaborative filtering recommendations"
    category = "SELECT"

    def setup_postgres(self, conn, ctx):
        self._pid = ctx.random_id("profiles")

    setup_mysql = setup_postgres
    setup_mongo = setup_postgres
    setup_neo4j = setup_postgres

    def run_postgres(self, conn, ctx):
        conn.execute("""
            WITH similar_profiles AS (
                SELECT DISTINCT wh2.profile_id
                FROM watch_history wh1
                JOIN watch_history wh2
                    ON wh1.content_id = wh2.content_id
                    AND wh1.profile_id <> wh2.profile_id
                WHERE wh1.profile_id = %s
                LIMIT 50
            )
            SELECT c.content_id, c.title, COUNT(*) AS score
            FROM watch_history wh
            JOIN content c ON wh.content_id = c.content_id
            WHERE wh.profile_id IN (SELECT profile_id FROM similar_profiles)
              AND wh.content_id NOT IN (
                  SELECT content_id FROM watch_history WHERE profile_id = %s
              )
            GROUP BY c.content_id, c.title
            ORDER BY score DESC
            LIMIT 10
        """, (self._pid, self._pid)).fetchall()

    def run_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute("""
            WITH similar_profiles AS (
                SELECT DISTINCT wh2.profile_id
                FROM watch_history wh1
                JOIN watch_history wh2
                    ON wh1.content_id = wh2.content_id
                    AND wh1.profile_id <> wh2.profile_id
                WHERE wh1.profile_id = %s
                LIMIT 50
            )
            SELECT c.content_id, c.title, COUNT(*) AS score
            FROM watch_history wh
            JOIN content c ON wh.content_id = c.content_id
            WHERE wh.profile_id IN (SELECT profile_id FROM similar_profiles)
              AND wh.content_id NOT IN (
                  SELECT content_id FROM watch_history WHERE profile_id = %s
              )
            GROUP BY c.content_id, c.title
            ORDER BY score DESC
            LIMIT 10
        """, (self._pid, self._pid))
        cur.fetchall()

    def run_mongo(self, db, ctx):
        watched = db.watch_history.distinct("content_id", {"profile_id": self._pid})
        similar = db.watch_history.distinct(
            "profile_id",
            {"content_id": {"$in": watched[:50]}, "profile_id": {"$ne": self._pid}},
        )[:50]
        list(db.watch_history.aggregate([
            {"$match": {
                "profile_id": {"$in": similar},
                "content_id": {"$nin": watched},
            }},
            {"$group": {"_id": "$content_id", "score": {"$sum": 1}}},
            {"$sort": {"score": -1}},
            {"$limit": 10},
            {"$lookup": {
                "from": "content", "localField": "_id",
                "foreignField": "_id", "as": "content",
            }},
        ]))

    def run_neo4j(self, driver, ctx):
        with driver.session() as s:
            s.run("""
                MATCH (p:Profile {profile_id: $pid})-[:WATCHED]->(c:Content)
                      <-[:WATCHED]-(similar:Profile)
                WHERE similar <> p
                WITH p, similar, COUNT(c) AS shared
                ORDER BY shared DESC LIMIT 50
                MATCH (similar)-[:WATCHED]->(rec:Content)
                WHERE NOT EXISTS { (p)-[:WATCHED]->(rec) }
                RETURN rec.content_id, rec.title, COUNT(*) AS score
                ORDER BY score DESC LIMIT 10
            """, pid=self._pid).consume()


class S3_Top100Viewership(BaseScenario):
    id = "S3"
    name = "TOP 100 content by recent viewership"
    category = "SELECT"

    def run_postgres(self, conn, ctx):
        conn.execute("""
            SELECT c.content_id, c.title, COUNT(*) AS views
            FROM watch_history wh
            JOIN content c ON wh.content_id = c.content_id
            WHERE wh.started_at >= '2025-06-01'
            GROUP BY c.content_id, c.title
            ORDER BY views DESC
            LIMIT 100
        """).fetchall()

    def run_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute("""
            SELECT c.content_id, c.title, COUNT(*) AS views
            FROM watch_history wh
            JOIN content c ON wh.content_id = c.content_id
            WHERE wh.started_at >= '2025-06-01'
            GROUP BY c.content_id, c.title
            ORDER BY views DESC
            LIMIT 100
        """)
        cur.fetchall()

    def run_mongo(self, db, ctx):
        list(db.watch_history.aggregate([
            {"$match": {"started_at": {"$gte": "2025-06-01"}}},
            {"$group": {"_id": "$content_id", "views": {"$sum": 1}}},
            {"$sort": {"views": -1}},
            {"$limit": 100},
            {"$lookup": {
                "from": "content", "localField": "_id",
                "foreignField": "_id", "as": "content",
            }},
        ]))

    def run_neo4j(self, driver, ctx):
        with driver.session() as s:
            s.run("""
                MATCH (p:Profile)-[w:WATCHED]->(c:Content)
                WHERE w.started_at >= '2025-06-01'
                RETURN c.content_id, c.title, COUNT(*) AS views
                ORDER BY views DESC
                LIMIT 100
            """).consume()


class S4_FullTextSearch(BaseScenario):
    id = "S4"
    name = "Full-text search on title"
    category = "SELECT"

    def run_postgres(self, conn, ctx):
        term = ctx.params["search_term"]
        conn.execute("""
            SELECT content_id, title, popularity_score,
                   metadata->>'tags' AS tags
            FROM content
            WHERE title ILIKE %s
            ORDER BY popularity_score DESC
            LIMIT 20
        """, (f"%{term}%",)).fetchall()

    def run_mysql(self, conn, ctx):
        term = ctx.params["search_term"]
        cur = conn.cursor()
        cur.execute("""
            SELECT content_id, title, popularity_score,
                   metadata->>'$.tags' AS tags
            FROM content
            WHERE title LIKE %s
            ORDER BY popularity_score DESC
            LIMIT 20
        """, (f"%{term}%",))
        cur.fetchall()

    def run_mongo(self, db, ctx):
        import re as _re
        from pymongo.errors import OperationFailure

        term = ctx.params["search_term"]
        try:
            list(db.content.find(
                {"$text": {"$search": term}},
                {"score": {"$meta": "textScore"}, "title": 1,
                 "popularity_score": 1, "metadata.tags": 1},
            ).sort([("score", {"$meta": "textScore"})]).limit(20))
        except OperationFailure:
            pattern = _re.compile(_re.escape(term), _re.IGNORECASE)
            list(db.content.find(
                {"title": {"$regex": pattern}},
                {"title": 1, "popularity_score": 1, "metadata.tags": 1},
            ).sort("popularity_score", -1).limit(20))

    def run_neo4j(self, driver, ctx):
        term = ctx.params["search_term"]
        with driver.session() as s:
            s.run("""
                MATCH (c:Content)
                WHERE c.title CONTAINS $term
                RETURN c.content_id, c.title, c.popularity_score
                ORDER BY c.popularity_score DESC
                LIMIT 20
            """, term=term).consume()


class S5_WatchHistory(BaseScenario):
    id = "S5"
    name = "Profile watch history with content titles"
    category = "SELECT"

    def setup_postgres(self, conn, ctx):
        self._pid = ctx.random_id("profiles")

    setup_mysql = setup_postgres
    setup_mongo = setup_postgres
    setup_neo4j = setup_postgres

    def run_postgres(self, conn, ctx):
        conn.execute("""
            SELECT wh.watch_id, c.title, c.type, wh.started_at,
                   wh.progress_percent, wh.completed
            FROM watch_history wh
            JOIN content c ON wh.content_id = c.content_id
            WHERE wh.profile_id = %s
            ORDER BY wh.started_at DESC
            LIMIT 50
        """, (self._pid,)).fetchall()

    def run_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute("""
            SELECT wh.watch_id, c.title, c.type, wh.started_at,
                   wh.progress_percent, wh.completed
            FROM watch_history wh
            JOIN content c ON wh.content_id = c.content_id
            WHERE wh.profile_id = %s
            ORDER BY wh.started_at DESC
            LIMIT 50
        """, (self._pid,))
        cur.fetchall()

    def run_mongo(self, db, ctx):
        list(db.watch_history.aggregate([
            {"$match": {"profile_id": self._pid}},
            {"$sort": {"started_at": -1}},
            {"$limit": 50},
            {"$lookup": {
                "from": "content", "localField": "content_id",
                "foreignField": "_id", "as": "content",
            }},
            {"$unwind": "$content"},
            {"$project": {
                "content.title": 1, "content.type": 1,
                "started_at": 1, "progress_percent": 1, "completed": 1,
            }},
        ]))

    def run_neo4j(self, driver, ctx):
        with driver.session() as s:
            s.run("""
                MATCH (p:Profile {profile_id: $pid})-[w:WATCHED]->(c:Content)
                RETURN c.title, c.type, w.started_at,
                       w.progress_percent, w.completed
                ORDER BY w.started_at DESC
                LIMIT 50
            """, pid=self._pid).consume()


class S6_Filmography(BaseScenario):
    id = "S6"
    name = "Person filmography"
    category = "SELECT"

    def setup_postgres(self, conn, ctx):
        self._person_id = ctx.random_id("people")

    setup_mysql = setup_postgres
    setup_mongo = setup_postgres
    setup_neo4j = setup_postgres

    def run_postgres(self, conn, ctx):
        conn.execute("""
            SELECT c.content_id, c.title, c.type, c.release_date,
                   cp.role, cp.character_name
            FROM content_people cp
            JOIN content c ON cp.content_id = c.content_id
            WHERE cp.person_id = %s
            ORDER BY c.release_date DESC
        """, (self._person_id,)).fetchall()

    def run_mysql(self, conn, ctx):
        cur = conn.cursor()
        cur.execute("""
            SELECT c.content_id, c.title, c.type, c.release_date,
                   cp.role, cp.character_name
            FROM content_people cp
            JOIN content c ON cp.content_id = c.content_id
            WHERE cp.person_id = %s
            ORDER BY c.release_date DESC
        """, (self._person_id,))
        cur.fetchall()

    def run_mongo(self, db, ctx):
        list(db.content.find(
            {"cast.person_id": self._person_id},
            {"title": 1, "type": 1, "release_date": 1, "cast.$": 1},
        ).sort("release_date", -1))

    def run_neo4j(self, driver, ctx):
        with driver.session() as s:
            s.run("""
                MATCH (p:Person {person_id: $pid})-[r]->(c:Content)
                WHERE type(r) IN ['ACTED_IN', 'DIRECTED', 'WROTE']
                RETURN c.content_id, c.title, c.type, c.release_date,
                       type(r) AS role, r.character_name
                ORDER BY c.release_date DESC
            """, pid=self._person_id).consume()


SELECT_SCENARIOS = [
    S1_Homepage(),
    S2_CollaborativeFiltering(),
    S3_Top100Viewership(),
    S4_FullTextSearch(),
    S5_WatchHistory(),
    S6_Filmography(),
]
