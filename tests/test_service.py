"""End-to-end service tests: the full read path (cache -> trie -> rank), the write
path (search -> buffer -> flush -> reflected), input normalisation, and the
basic-vs-trending difference."""

import asyncio
import os
import tempfile
import unittest

from app.config import Settings
from app.service import SuggestionService, normalize

SEED = [
    ("apple", 100), ("application", 80), ("apply", 60), ("appetite", 30),
    ("banana", 50), ("band", 40), ("bandana", 20),
]


class TestNormalize(unittest.TestCase):
    def test_normalize(self):
        self.assertEqual(normalize("  IPhone 15  "), "iphone 15")
        self.assertEqual(normalize("A\tB\nC"), "a b c")
        self.assertEqual(normalize(None), "")
        self.assertEqual(normalize(""), "")


class TestService(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        db = os.path.join(self.tmp.name, "t.db")
        wal = os.path.join(self.tmp.name, "buffer.wal")
        # Pre-populate the primary store.
        from app.storage import Storage
        s = Storage(db)
        s.bulk_replace(SEED)
        s.close()
        self.settings = Settings(
            db_path=db, wal_path=wal, batch_flush_interval=999, batch_max_size=10_000,
            recency_half_life=10_000, default_ranking_mode="basic", trie_node_capacity=10,
        )
        self.svc = SuggestionService(self.settings)
        self.svc.load_index()
        self.svc.recover()

    def tearDown(self):
        asyncio.run(self.svc.shutdown())
        self.tmp.cleanup()

    def test_suggest_basic_sorted_by_count(self):
        r = self.svc.suggest("app", mode="basic")
        self.assertEqual([s["query"] for s in r["suggestions"]],
                         ["apple", "application", "apply", "appetite"])
        self.assertEqual(r["cache"], "miss")

    def test_prefix_only_matches(self):
        r = self.svc.suggest("ban", mode="basic")
        for s in r["suggestions"]:
            self.assertTrue(s["query"].startswith("ban"))

    def test_normalization_mixed_case_and_space(self):
        a = self.svc.suggest("APP")["suggestions"]
        b = self.svc.suggest("  app ")["suggestions"]
        self.assertEqual([x["query"] for x in a], [x["query"] for x in b])

    def test_empty_and_no_match(self):
        self.assertEqual(self.svc.suggest("")["suggestions"], [])
        self.assertEqual(self.svc.suggest("")["cache"], "bypass")
        self.assertEqual(self.svc.suggest("zzz")["suggestions"], [])

    def test_cache_hit_on_repeat(self):
        self.assertEqual(self.svc.suggest("app")["cache"], "miss")
        self.assertEqual(self.svc.suggest("app")["cache"], "hit")

    def test_limit_honored_across_cache_hits(self):
        # 'app' has 4 matches. Different limits for the same prefix must each be
        # honored, even though the second call is a cache hit (regression test
        # for the cache key omitting limit).
        r2 = self.svc.suggest("app", limit=2)
        self.assertEqual(r2["cache"], "miss")
        self.assertEqual(r2["count"], 2)
        r4 = self.svc.suggest("app", limit=4)
        self.assertEqual(r4["cache"], "hit")     # served from the same cached pool
        self.assertEqual(r4["count"], 4)
        # reverse direction too: 'appl' matches apple, application, apply (3)
        r3 = self.svc.suggest("appl", limit=4)   # ask for more than exist
        self.assertEqual(r3["count"], 3)
        r1 = self.svc.suggest("appl", limit=1)   # cache hit, smaller limit
        self.assertEqual(r1["cache"], "hit")
        self.assertEqual(r1["count"], 1)

    def test_search_updates_counts_after_flush(self):
        for _ in range(25):
            self.svc.search("appetite")          # least popular 'app' query
        asyncio.run(self.svc.batch_writer.flush())
        # 30 + 25 = 55 -> now above 'apply' (60? no) ; check the count moved.
        self.assertEqual(self.svc.trie.get_count("appetite"), 55)
        # cache was invalidated, so a fresh basic suggest recomputes
        r = self.svc.suggest("app", mode="basic")
        self.assertEqual(r["cache"], "miss")

    def test_new_query_inserted_and_searchable(self):
        r = self.svc.search("apricot")
        self.assertTrue(r["recorded"])
        asyncio.run(self.svc.batch_writer.flush())
        self.assertEqual(self.svc.trie.get_count("apricot"), 1)
        self.assertIn("apricot", [s["query"] for s in self.svc.suggest("apr")["suggestions"]])

    def test_trending_surfaces_surging_query(self):
        # 'bandana' (count 20) is the weakest 'ban' query. Burst it modestly:
        # 20 + 8 = 28, still below 'band' (40) and 'banana' (50), so the BASIC
        # (count) order is unchanged — isolating the recency effect.
        for _ in range(8):
            self.svc.search("bandana")
        asyncio.run(self.svc.batch_writer.flush())
        basic = [s["query"] for s in self.svc.suggest("ban", mode="basic")["suggestions"]]
        trending = [s["query"] for s in self.svc.suggest("ban", mode="trending")["suggestions"]]
        # basic still ranks by count (banana ahead); trending lifts bandana to #1.
        self.assertEqual(basic[0], "banana")
        self.assertEqual(trending[0], "bandana")

    def test_metrics_shape(self):
        self.svc.suggest("app")
        self.svc.search("apple")
        m = self.svc.metrics_summary()
        self.assertIn("latency", m["requests"])
        self.assertIn("hit_rate", m["cache"])
        self.assertIn("write_reduction_ratio", m["batch_writer"])
        self.assertEqual(m["index"]["queries"], len(self.svc.trie))


if __name__ == "__main__":
    unittest.main()
