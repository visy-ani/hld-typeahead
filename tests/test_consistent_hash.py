"""Consistent-hash ring: deterministic routing, even spread with virtual nodes,
and the defining property — removing a node remaps only ~1/N of keys."""

import unittest

from app.consistent_hash import ConsistentHashRing


class TestConsistentHash(unittest.TestCase):
    def test_deterministic_routing(self):
        ring = ConsistentHashRing(["a", "b", "c"], vnodes=100)
        self.assertEqual(ring.get_node("hello"), ring.get_node("hello"))

    def test_all_keys_route_to_real_nodes(self):
        ring = ConsistentHashRing(["a", "b", "c", "d"], vnodes=80)
        nodes = set(ring.nodes)
        for i in range(2000):
            self.assertIn(ring.get_node(f"k{i}"), nodes)

    def test_even_distribution(self):
        ring = ConsistentHashRing([f"n{i}" for i in range(5)], vnodes=200)
        keys = [f"prefix-{i}" for i in range(50000)]
        dist = ring.distribution(keys)
        ideal = len(keys) / 5
        # With 200 vnodes each, every node should be within ~25% of the ideal.
        for n, c in dist.items():
            self.assertLess(abs(c - ideal) / ideal, 0.25, f"{n} too skewed: {c} vs {ideal}")

    def test_minimal_remap_on_removal(self):
        """The whole point of consistent hashing: removing 1 of N nodes should
        move only ~1/N of keys, not nearly all of them (as hash%N would)."""
        ring = ConsistentHashRing(["a", "b", "c", "d", "e"], vnodes=200)
        keys = [f"key-{i}" for i in range(50000)]
        before = {k: ring.get_node(k) for k in keys}
        ring.remove_node("c")
        moved = sum(1 for k in keys if ring.get_node(k) != before[k])
        frac = moved / len(keys)
        # Only keys that were on 'c' should move: ~1/5 = 0.2.
        self.assertLess(frac, 0.30, f"too many keys moved: {frac:.3f}")
        # And keys NOT on 'c' must keep their owner.
        for k in keys:
            if before[k] != "c":
                self.assertEqual(ring.get_node(k), before[k])

    def test_minimal_remap_on_addition(self):
        ring = ConsistentHashRing(["a", "b", "c", "d"], vnodes=200)
        keys = [f"key-{i}" for i in range(40000)]
        before = {k: ring.get_node(k) for k in keys}
        ring.add_node("e")
        moved = sum(1 for k in keys if ring.get_node(k) != before[k])
        frac = moved / len(keys)
        # Adding the 5th node should pull ~1/5 of keys onto it.
        self.assertLess(frac, 0.30, f"too many keys moved: {frac:.3f}")
        self.assertGreater(frac, 0.10, f"suspiciously few keys moved: {frac:.3f}")

    def test_add_is_idempotent(self):
        ring = ConsistentHashRing(["a"], vnodes=10)
        size = ring.ring_size
        ring.add_node("a")
        self.assertEqual(ring.ring_size, size)

    def test_empty_ring_raises(self):
        ring = ConsistentHashRing([], vnodes=10)
        with self.assertRaises(RuntimeError):
            ring.get_node("x")

    def test_ring_size(self):
        ring = ConsistentHashRing(["a", "b"], vnodes=50)
        self.assertEqual(ring.ring_size, 100)


if __name__ == "__main__":
    unittest.main()
