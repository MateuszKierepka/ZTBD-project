import json
import traceback
from pathlib import Path

import psycopg
import pymysql
from pymongo import MongoClient
from neo4j import GraphDatabase


SCENARIOS = [
    ("S1", "Homepage: filter+sort by popularity"),
    ("S2", "Collaborative filtering recommendations"),
    ("S3", "TOP 100 content by recent viewership"),
    ("S4", "Full-text search on title"),
    ("S5", "Profile watch history with content titles"),
    ("S6", "Person filmography"),
]


class ExplainAnalyzer:

    def __init__(self, results_dir: Path):
        self.results_dir = results_dir / "explain"
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.connections: dict = {}

    def connect(self) -> None:
        self.connections["postgres"] = psycopg.connect(
            "host=localhost port=5432 dbname=vod user=vod password=vod123"
        )
        self.connections["mysql"] = pymysql.connect(
            host="localhost", port=3306, user="vod",
            password="vod123", database="vod",
        )
        client = MongoClient("localhost", 27017)
        self.connections["mongo"] = client["vod"]
        self._mongo_client = client
        self.connections["neo4j"] = GraphDatabase.driver(
            "bolt://localhost:7687", auth=("neo4j", "vod12345")
        )

    def close(self) -> None:
        self.connections["postgres"].close()
        self.connections["mysql"].close()
        self._mongo_client.close()
        self.connections["neo4j"].close()

    def analyze_all(self, volume: str, with_indexes: bool) -> Path:
        label = "with_indexes" if with_indexes else "no_indexes"
        ctx = self._build_context()
        all_plans = {}

        for sid, name in SCENARIOS:
            print(f"\n  [{sid}] {name}")
            all_plans[sid] = {"name": name, "plans": {}, "summary": {}}

            for db_name in ("postgres", "mysql", "mongo", "neo4j"):
                print(f"    {db_name}...", end=" ")
                try:
                    plan = self._run_explain(db_name, sid, ctx)
                    summary = self._extract_summary(db_name, plan)
                    all_plans[sid]["plans"][db_name] = plan
                    all_plans[sid]["summary"][db_name] = summary
                    print("OK")
                except Exception as exc:
                    print(f"ERROR: {exc}")
                    traceback.print_exc()
                    all_plans[sid]["plans"][db_name] = {"error": str(exc)}
                    all_plans[sid]["summary"][db_name] = {"error": str(exc)}

        filepath = self.results_dir / f"explain_{volume}_{label}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(all_plans, f, indent=2, ensure_ascii=False, default=str)
        print(f"\n  Plans saved to {filepath}")

        self._print_summary_table(all_plans)
        return filepath

    def _build_context(self) -> dict:
        conn = self.connections["postgres"]
        ctx = {}
        row = conn.execute(
            "SELECT profile_id FROM profiles ORDER BY RANDOM() LIMIT 1"
        ).fetchone()
        ctx["profile_id"] = row[0]

        row = conn.execute(
            "SELECT person_id FROM people ORDER BY RANDOM() LIMIT 1"
        ).fetchone()
        ctx["person_id"] = row[0]

        ctx["search_term"] = "Interface"
        return ctx

    def _run_explain(self, db_name: str, sid: str, ctx: dict):
        return getattr(self, f"_explain_{db_name}")(sid, ctx)

    def _explain_postgres(self, sid: str, ctx: dict):
        conn = self.connections["postgres"]
        queries = {
            "S1": ("""
                EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
                SELECT content_id, title, genres, popularity_score, avg_rating,
                       metadata->>'studio' AS studio
                FROM content
                WHERE is_active = TRUE AND genres LIKE '%%Action%%'
                ORDER BY popularity_score DESC
                LIMIT 20
            """, None),
            "S2": ("""
                EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
                WITH similar_profiles AS (
                    SELECT DISTINCT wh2.profile_id
                    FROM watch_history wh1
                    JOIN watch_history wh2 ON wh1.content_id = wh2.content_id
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
                ORDER BY score DESC LIMIT 10
            """, (ctx["profile_id"], ctx["profile_id"])),
            "S3": ("""
                EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
                SELECT c.content_id, c.title, COUNT(*) AS views
                FROM watch_history wh
                JOIN content c ON wh.content_id = c.content_id
                WHERE wh.started_at >= '2025-06-01'
                GROUP BY c.content_id, c.title
                ORDER BY views DESC LIMIT 100
            """, None),
            "S4": ("""
                EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
                SELECT content_id, title, popularity_score,
                       metadata->>'tags' AS tags
                FROM content
                WHERE title ILIKE %s
                ORDER BY popularity_score DESC LIMIT 20
            """, (f"%{ctx['search_term']}%",)),
            "S5": ("""
                EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
                SELECT wh.watch_id, c.title, c.type, wh.started_at,
                       wh.progress_percent, wh.completed
                FROM watch_history wh
                JOIN content c ON wh.content_id = c.content_id
                WHERE wh.profile_id = %s
                ORDER BY wh.started_at DESC LIMIT 50
            """, (ctx["profile_id"],)),
            "S6": ("""
                EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
                SELECT c.content_id, c.title, c.type, c.release_date,
                       cp.role, cp.character_name
                FROM content_people cp
                JOIN content c ON cp.content_id = c.content_id
                WHERE cp.person_id = %s
                ORDER BY c.release_date DESC
            """, (ctx["person_id"],)),
        }
        query, params = queries[sid]
        if params:
            row = conn.execute(query, params).fetchone()
        else:
            row = conn.execute(query).fetchone()
        return row[0]

    def _explain_mysql(self, sid: str, ctx: dict):
        conn = self.connections["mysql"]
        cur = conn.cursor()
        queries = {
            "S1": ("""
                EXPLAIN FORMAT=JSON
                SELECT content_id, title, genres, popularity_score, avg_rating,
                       metadata->>'$.studio' AS studio
                FROM content
                WHERE is_active = TRUE AND genres LIKE '%%Action%%'
                ORDER BY popularity_score DESC
                LIMIT 20
            """, None),
            "S2": ("""
                EXPLAIN FORMAT=JSON
                WITH similar_profiles AS (
                    SELECT DISTINCT wh2.profile_id
                    FROM watch_history wh1
                    JOIN watch_history wh2 ON wh1.content_id = wh2.content_id
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
                ORDER BY score DESC LIMIT 10
            """, (ctx["profile_id"], ctx["profile_id"])),
            "S3": ("""
                EXPLAIN FORMAT=JSON
                SELECT c.content_id, c.title, COUNT(*) AS views
                FROM watch_history wh
                JOIN content c ON wh.content_id = c.content_id
                WHERE wh.started_at >= '2025-06-01'
                GROUP BY c.content_id, c.title
                ORDER BY views DESC LIMIT 100
            """, None),
            "S4": ("""
                EXPLAIN FORMAT=JSON
                SELECT content_id, title, popularity_score,
                       metadata->>'$.tags' AS tags
                FROM content
                WHERE title LIKE %s
                ORDER BY popularity_score DESC LIMIT 20
            """, (f"%{ctx['search_term']}%",)),
            "S5": ("""
                EXPLAIN FORMAT=JSON
                SELECT wh.watch_id, c.title, c.type, wh.started_at,
                       wh.progress_percent, wh.completed
                FROM watch_history wh
                JOIN content c ON wh.content_id = c.content_id
                WHERE wh.profile_id = %s
                ORDER BY wh.started_at DESC LIMIT 50
            """, (ctx["profile_id"],)),
            "S6": ("""
                EXPLAIN FORMAT=JSON
                SELECT c.content_id, c.title, c.type, c.release_date,
                       cp.role, cp.character_name
                FROM content_people cp
                JOIN content c ON cp.content_id = c.content_id
                WHERE cp.person_id = %s
                ORDER BY c.release_date DESC
            """, (ctx["person_id"],)),
        }
        query, params = queries[sid]
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        row = cur.fetchone()
        return json.loads(row[0])

    def _explain_mongo(self, sid: str, ctx: dict):
        db = self.connections["mongo"]

        if sid == "S1":
            return db.content.find(
                {"is_active": True, "genres": "Action"},
                {"title": 1, "genres": 1, "popularity_score": 1,
                 "avg_rating": 1, "metadata.studio": 1},
            ).sort("popularity_score", -1).limit(20).explain()

        if sid == "S2":
            return db.command("explain", {
                "aggregate": "watch_history",
                "pipeline": [
                    {"$match": {"profile_id": ctx["profile_id"]}},
                    {"$group": {"_id": "$content_id", "score": {"$sum": 1}}},
                    {"$sort": {"score": -1}},
                    {"$limit": 10},
                ],
                "cursor": {},
            }, verbosity="executionStats")

        if sid == "S3":
            return db.command("explain", {
                "aggregate": "watch_history",
                "pipeline": [
                    {"$match": {"started_at": {"$gte": "2025-06-01"}}},
                    {"$group": {"_id": "$content_id", "views": {"$sum": 1}}},
                    {"$sort": {"views": -1}},
                    {"$limit": 100},
                    {"$lookup": {
                        "from": "content", "localField": "_id",
                        "foreignField": "_id", "as": "content",
                    }},
                ],
                "cursor": {},
            }, verbosity="executionStats")

        if sid == "S4":
            term = ctx["search_term"]
            try:
                return db.content.find(
                    {"$text": {"$search": term}},
                    {"score": {"$meta": "textScore"}, "title": 1,
                     "popularity_score": 1, "metadata.tags": 1},
                ).sort([("score", {"$meta": "textScore"})]).limit(20).explain()
            except Exception:
                return db.content.find(
                    {"title": {"$regex": term, "$options": "i"}},
                    {"title": 1, "popularity_score": 1, "metadata.tags": 1},
                ).limit(20).explain()

        if sid == "S5":
            return db.command("explain", {
                "aggregate": "watch_history",
                "pipeline": [
                    {"$match": {"profile_id": ctx["profile_id"]}},
                    {"$sort": {"started_at": -1}},
                    {"$limit": 50},
                    {"$lookup": {
                        "from": "content", "localField": "content_id",
                        "foreignField": "_id", "as": "content",
                    }},
                    {"$unwind": "$content"},
                ],
                "cursor": {},
            }, verbosity="executionStats")

        if sid == "S6":
            return db.content.find(
                {"cast.person_id": ctx["person_id"]},
                {"title": 1, "type": 1, "release_date": 1, "cast.$": 1},
            ).sort("release_date", -1).explain()

    def _explain_neo4j(self, sid: str, ctx: dict):
        driver = self.connections["neo4j"]
        queries = {
            "S1": ("""
                PROFILE
                MATCH (c:Content)-[:HAS_GENRE]->(g:Genre {name: 'Action'})
                WHERE c.is_active = true
                RETURN c.content_id, c.title, c.popularity_score, c.avg_rating
                ORDER BY c.popularity_score DESC LIMIT 20
            """, {}),
            "S2": ("""
                PROFILE
                MATCH (p:Profile {profile_id: $pid})-[:WATCHED]->(c:Content)
                      <-[:WATCHED]-(similar:Profile)
                WHERE similar <> p
                WITH p, similar, COUNT(c) AS shared
                ORDER BY shared DESC LIMIT 50
                MATCH (similar)-[:WATCHED]->(rec:Content)
                WHERE NOT EXISTS { (p)-[:WATCHED]->(rec) }
                RETURN rec.content_id, rec.title, COUNT(*) AS score
                ORDER BY score DESC LIMIT 10
            """, {"pid": ctx["profile_id"]}),
            "S3": ("""
                PROFILE
                MATCH (p:Profile)-[w:WATCHED]->(c:Content)
                WHERE w.started_at >= '2025-06-01'
                RETURN c.content_id, c.title, COUNT(*) AS views
                ORDER BY views DESC LIMIT 100
            """, {}),
            "S4": ("""
                PROFILE
                MATCH (c:Content)
                WHERE c.title CONTAINS $term
                RETURN c.content_id, c.title, c.popularity_score
                ORDER BY c.popularity_score DESC LIMIT 20
            """, {"term": ctx["search_term"]}),
            "S5": ("""
                PROFILE
                MATCH (p:Profile {profile_id: $pid})-[w:WATCHED]->(c:Content)
                RETURN c.title, c.type, w.started_at,
                       w.progress_percent, w.completed
                ORDER BY w.started_at DESC LIMIT 50
            """, {"pid": ctx["profile_id"]}),
            "S6": ("""
                PROFILE
                MATCH (p:Person {person_id: $pid})-[r]->(c:Content)
                WHERE type(r) IN ['ACTED_IN', 'DIRECTED', 'WROTE']
                RETURN c.content_id, c.title, c.type, c.release_date,
                       type(r) AS role, r.character_name
                ORDER BY c.release_date DESC
            """, {"pid": ctx["person_id"]}),
        }
        query, params = queries[sid]
        with driver.session() as s:
            result = s.run(query, params)
            summary = result.consume()
            plan = summary.profile
        return self._neo4j_plan_to_dict(plan)

    def _neo4j_plan_to_dict(self, plan) -> dict | None:
        if plan is None:
            return None
        if isinstance(plan, dict):
            return {
                "operator_type": plan.get("operatorType", ""),
                "arguments": plan.get("args", {}),
                "identifiers": plan.get("identifiers", []),
                "db_hits": plan.get("dbHits", 0),
                "rows": plan.get("rows", 0),
                "children": [
                    self._neo4j_plan_to_dict(c)
                    for c in plan.get("children", [])
                ],
            }
        return {
            "operator_type": plan.operator_type,
            "arguments": {k: v for k, v in plan.arguments.items()} if plan.arguments else {},
            "identifiers": list(plan.identifiers) if plan.identifiers else [],
            "db_hits": getattr(plan, "db_hits", 0),
            "rows": getattr(plan, "rows", 0),
            "children": [self._neo4j_plan_to_dict(c) for c in plan.children]
            if plan.children else [],
        }

    def _extract_summary(self, db_name: str, plan) -> dict:
        return getattr(self, f"_{db_name}_summary")(plan)

    def _postgres_summary(self, plan) -> dict:
        p = plan[0]
        scans = []
        self._pg_find_scans(p["Plan"], scans)
        return {
            "planning_time_ms": p.get("Planning Time", 0),
            "execution_time_ms": p.get("Execution Time", 0),
            "total_cost": p["Plan"].get("Total Cost", 0),
            "actual_rows": p["Plan"].get("Actual Rows", 0),
            "scan_types": scans,
        }

    def _pg_find_scans(self, node: dict, scans: list) -> None:
        nt = node.get("Node Type", "")
        if "Scan" in nt or "Search" in nt:
            scans.append({
                "type": nt,
                "relation": node.get("Relation Name", ""),
                "index": node.get("Index Name"),
                "rows": node.get("Actual Rows", 0),
            })
        for child in node.get("Plans", []):
            self._pg_find_scans(child, scans)

    def _mysql_summary(self, plan) -> dict:
        tables = []
        self._mysql_find_tables(plan, tables)
        return {"tables": tables}

    def _mysql_find_tables(self, node: dict, tables: list) -> None:
        if "table" in node:
            t = node["table"]
            tables.append({
                "table_name": t.get("table_name", ""),
                "access_type": t.get("access_type", ""),
                "key": t.get("key"),
                "rows_examined": t.get("rows_examined_per_scan", t.get("rows", 0)),
                "filtered": t.get("filtered", ""),
            })
        for key in ("query_block", "ordering_operation", "grouping_operation",
                     "duplicates_removal", "nested_loop", "table"):
            sub = node.get(key)
            if isinstance(sub, dict):
                self._mysql_find_tables(sub, tables)
            elif isinstance(sub, list):
                for item in sub:
                    if isinstance(item, dict):
                        self._mysql_find_tables(item, tables)

    def _mongo_summary(self, plan) -> dict:
        stats = plan.get("executionStats", {})
        if stats:
            return {
                "n_returned": stats.get("nReturned", 0),
                "total_docs_examined": stats.get("totalDocsExamined", 0),
                "total_keys_examined": stats.get("totalKeysExamined", 0),
                "execution_time_ms": stats.get("executionTimeMillis", 0),
                "scan_type": self._mongo_find_stage(
                    stats.get("executionStages", {})),
            }
        stages = plan.get("stages", [])
        if stages:
            first = stages[0].get("$cursor", {}).get("executionStats", {})
            return {
                "n_returned": first.get("nReturned", 0),
                "total_docs_examined": first.get("totalDocsExamined", 0),
                "total_keys_examined": first.get("totalKeysExamined", 0),
                "execution_time_ms": first.get("executionTimeMillis", 0),
                "scan_type": self._mongo_find_stage(
                    first.get("executionStages", {})),
            }
        return {"raw_keys": list(plan.keys())}

    def _mongo_find_stage(self, stage: dict) -> str:
        s = stage.get("stage", "")
        if s in ("COLLSCAN", "IXSCAN", "TEXT_OR", "TEXT_MATCH"):
            return s
        input_stage = stage.get("inputStage")
        if input_stage:
            return self._mongo_find_stage(input_stage)
        return s or "UNKNOWN"

    def _neo4j_summary(self, plan) -> dict:
        if plan is None:
            return {"error": "no profile data"}
        total_hits = [0]
        operators = []
        self._neo4j_collect(plan, total_hits, operators)
        return {
            "total_db_hits": total_hits[0],
            "result_rows": plan.get("rows", 0),
            "operators": operators,
        }

    def _neo4j_collect(self, node: dict, hits_ref: list, ops: list) -> None:
        hits_ref[0] += node.get("db_hits", 0)
        ops.append(node.get("operator_type", ""))
        for child in node.get("children", []):
            self._neo4j_collect(child, hits_ref, ops)

    def _print_summary_table(self, all_plans: dict) -> None:
        print(f"\n{'='*80}")
        print("  EXPLAIN SUMMARY")
        print(f"{'='*80}")
        for sid, data in all_plans.items():
            print(f"\n  [{sid}] {data['name']}")
            for db_name, summary in data.get("summary", {}).items():
                if "error" in summary:
                    print(f"    {db_name:<10} ERROR: {summary['error']}")
                    continue
                if db_name == "postgres":
                    scans = ", ".join(
                        f"{s['type']}({s.get('index') or s['relation']})"
                        for s in summary.get("scan_types", [])
                    )
                    print(f"    {db_name:<10} exec={summary['execution_time_ms']:.2f}ms  "
                          f"scans=[{scans}]")
                elif db_name == "mysql":
                    tables = ", ".join(
                        f"{t['access_type']}({t.get('key') or t['table_name']})"
                        for t in summary.get("tables", [])
                    )
                    print(f"    {db_name:<10} access=[{tables}]")
                elif db_name == "mongo":
                    print(f"    {db_name:<10} exec={summary.get('execution_time_ms', '?')}ms  "
                          f"scan={summary.get('scan_type', '?')}  "
                          f"docs={summary.get('total_docs_examined', '?')}  "
                          f"keys={summary.get('total_keys_examined', '?')}")
                elif db_name == "neo4j":
                    print(f"    {db_name:<10} db_hits={summary.get('total_db_hits', '?')}  "
                          f"rows={summary.get('result_rows', '?')}  "
                          f"ops={summary.get('operators', [])}")
        print(f"\n{'='*80}")
