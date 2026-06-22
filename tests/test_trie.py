"""Trie correctness: the materialised per-node top-K must equal the brute-force
top-K, both after bulk load and after incremental upserts."""

import random
import unittest

from app.trie import Trie


def brute_top(items, prefix, k):
    """Reference implementation: filter by prefix, sort by (count desc, query asc)."""
    matches = [(q, c) for q, c in items if q.startswith(prefix)]
    matches.sort(key=lambda e: (-e[1], e[0]))
    return matches[:k]


class TestTrieBulk(unittest.TestCase):
    def setUp(self):
        rng = random.Random(7)
        letters = "abcde"
        self.items = {}
        for _ in range(3000):
            n = rng.randint(1, 6)
            w = "".join(rng.choice(letters) for _ in range(n))
            self.items[w] = rng.randint(1, 1_000_000)
        self.items = list(self.items.items())
        self.trie = Trie(capacity=10)
        self.trie.bulk_load(self.items)

    def test_size_and_contains(self):
        self.assertEqual(len(self.trie), len(self.items))
        for q, c in self.items[:50]:
            self.assertTrue(self.trie.contains(q))
            self.assertEqual(self.trie.get_count(q), c)
        self.assertFalse(self.trie.contains("zzzzzz"))
        self.assertIsNone(self.trie.get_count("zzzzzz"))

    def test_topk_matches_bruteforce(self):
        prefixes = [""] + [c for c in "abcde"] + ["ab", "cd", "abc", "bcd", "eee"]
        for p in prefixes:
            got = self.trie.top(p, 10)
            want = brute_top(self.items, p, 10)
            self.assertEqual(got, want, f"prefix {p!r}")

    def test_no_match_and_empty(self):
        self.assertEqual(self.trie.top("zzz", 10), [])
        # empty prefix returns the global top-K
        self.assertEqual(self.trie.top("", 10), brute_top(self.items, "", 10))

    def test_capacity_is_respected(self):
        # every node keeps at most `capacity` candidates
        # (checked indirectly: top() never returns more than capacity)
        self.assertLessEqual(len(self.trie.candidates("")), 10)


class TestTrieUpsert(unittest.TestCase):
    def test_incremental_matches_bruteforce(self):
        rng = random.Random(11)
        letters = "abc"
        counts = {}
        trie = Trie(capacity=8)
        # Start empty, apply a stream of monotonic increments via upsert.
        for _ in range(4000):
            n = rng.randint(1, 5)
            w = "".join(rng.choice(letters) for _ in range(n))
            counts[w] = counts.get(w, 0) + rng.randint(1, 50)
            trie.upsert(w, counts[w])
        items = list(counts.items())
        for p in ["", "a", "b", "c", "ab", "ba", "abc", "cba"]:
            self.assertEqual(trie.top(p, 8), brute_top(items, p, 8), f"prefix {p!r}")

    def test_upsert_promotes_into_topk(self):
        trie = Trie(capacity=2)
        trie.bulk_load([("aa", 10), ("ab", 9), ("ac", 8)])
        # 'ac' is outside the top-2 for 'a'. Bump it above the rest.
        self.assertEqual([q for q, _ in trie.top("a", 2)], ["aa", "ab"])
        trie.upsert("ac", 100)
        self.assertEqual(trie.top("a", 2), [("ac", 100), ("aa", 10)])

    def test_new_query_inserted(self):
        trie = Trie(capacity=5)
        trie.bulk_load([("cat", 5)])
        self.assertFalse(trie.contains("car"))
        trie.upsert("car", 3)
        self.assertTrue(trie.contains("car"))
        self.assertEqual(trie.get_count("car"), 3)
        self.assertIn(("car", 3), trie.top("ca", 5))


if __name__ == "__main__":
    unittest.main()
