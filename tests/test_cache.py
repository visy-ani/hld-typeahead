"""Cache: TTL expiry, LRU eviction, hit/miss accounting, and ring-routed
distributed get/set/invalidate. A fake clock makes time deterministic."""

import unittest

from app.cache import CacheNode, DistributedCache


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


class TestCacheNode(unittest.TestCase):
    def test_hit_then_miss_on_expiry(self):
        clock = Clock()
        node = CacheNode("n", capacity=10, ttl=30, time_fn=clock)
        node.set("k", [1, 2, 3])
        hit, val = node.get("k")
        self.assertTrue(hit)
        self.assertEqual(val, [1, 2, 3])
        clock.advance(31)
        hit, val = node.get("k")
        self.assertFalse(hit)
        self.assertEqual(node.stats.expirations, 1)

    def test_lru_eviction(self):
        clock = Clock()
        node = CacheNode("n", capacity=2, ttl=1000, time_fn=clock)
        node.set("a", 1)
        node.set("b", 2)
        node.get("a")            # 'a' is now most-recently used
        node.set("c", 3)         # evicts least-recently used => 'b'
        self.assertTrue(node.get("a")[0])
        self.assertFalse(node.get("b")[0])
        self.assertTrue(node.get("c")[0])
        self.assertEqual(node.stats.evictions, 1)

    def test_ttl_remaining_and_invalidate(self):
        clock = Clock()
        node = CacheNode("n", capacity=5, ttl=30, time_fn=clock)
        node.set("k", 1)
        self.assertAlmostEqual(node.ttl_remaining("k"), 30, places=3)
        clock.advance(10)
        self.assertAlmostEqual(node.ttl_remaining("k"), 20, places=3)
        self.assertTrue(node.invalidate("k"))
        self.assertIsNone(node.ttl_remaining("k"))


class TestDistributedCache(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.cache = DistributedCache(
            node_count=4, vnodes=100, ttl=30, capacity_per_node=1000, time_fn=self.clock
        )

    def test_set_get_routing(self):
        self.cache.set("iphone", "basic", ["a", "b"])
        hit, val = self.cache.get("iphone", "basic")
        self.assertTrue(hit)
        self.assertEqual(val, ["a", "b"])
        # same prefix+mode always routes to the same node
        n1 = self.cache.node_for("iphone", "basic").node_id
        n2 = self.cache.node_for("iphone", "basic").node_id
        self.assertEqual(n1, n2)

    def test_modes_dont_collide(self):
        self.cache.set("ip", "basic", ["basic-result"])
        self.cache.set("ip", "trending", ["trending-result"])
        self.assertEqual(self.cache.get("ip", "basic")[1], ["basic-result"])
        self.assertEqual(self.cache.get("ip", "trending")[1], ["trending-result"])

    def test_invalidate_all_modes(self):
        self.cache.set("ip", "basic", [1])
        self.cache.set("ip", "trending", [2])
        n = self.cache.invalidate_all_modes("ip")
        self.assertEqual(n, 2)
        self.assertFalse(self.cache.get("ip", "basic")[0])
        self.assertFalse(self.cache.get("ip", "trending")[0])

    def test_debug_reports_node_and_presence(self):
        info = self.cache.debug("london", "basic")
        self.assertFalse(info.present)
        self.cache.set("london", "basic", [1])
        info = self.cache.debug("london", "basic")
        self.assertTrue(info.present)
        self.assertIn(info.placement.node, self.cache.ring.nodes)
        self.assertEqual(info.placement.node, self.cache.node_for("london", "basic").node_id)

    def test_global_hit_rate(self):
        self.cache.set("a", "basic", [1])
        self.cache.get("a", "basic")   # hit
        self.cache.get("b", "basic")   # miss
        stats = self.cache.stats()
        self.assertEqual(stats["total_hits"], 1)
        self.assertEqual(stats["total_misses"], 1)
        self.assertAlmostEqual(stats["hit_rate"], 0.5)


if __name__ == "__main__":
    unittest.main()
