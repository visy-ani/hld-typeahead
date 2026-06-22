"""Ranking: exponential decay behaviour, prefix matching, the recency blend, and
the key property that a brief spike cannot permanently over-rank."""

import unittest

from app.ranking import RecencyTracker, blend


class Clock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


class TestRecencyDecay(unittest.TestCase):
    def test_halves_after_one_half_life(self):
        clock = Clock()
        rec = RecencyTracker(half_life=100, time_fn=clock)
        rec.record("q")                      # score = 1.0 at t=1000
        self.assertAlmostEqual(rec.score("q"), 1.0, places=6)
        clock.advance(100)                   # one half-life later
        self.assertAlmostEqual(rec.score("q"), 0.5, places=6)
        clock.advance(100)                   # two half-lives
        self.assertAlmostEqual(rec.score("q"), 0.25, places=6)

    def test_accumulates_with_decay(self):
        clock = Clock()
        rec = RecencyTracker(half_life=100, time_fn=clock)
        rec.record("q")          # 1.0
        clock.advance(100)       # decays to 0.5
        rec.record("q")          # 0.5 + 1.0 = 1.5
        self.assertAlmostEqual(rec.score("q"), 1.5, places=6)

    def test_unknown_query_scores_zero(self):
        rec = RecencyTracker(half_life=100, time_fn=Clock())
        self.assertEqual(rec.score("nope"), 0.0)

    def test_spike_does_not_permanently_dominate(self):
        """A query spiked hard once, then goes quiet, while another stays gently
        active. Eventually the steadily-active query must overtake the spike."""
        clock = Clock()
        rec = RecencyTracker(half_life=60, time_fn=clock)
        for _ in range(100):
            rec.record("spike")              # huge one-time burst
        # 'steady' gets a small amount of activity every minute.
        for _ in range(20):
            clock.advance(60)
            rec.record("steady")
        self.assertGreater(rec.score("steady"), rec.score("spike"))

    def test_matching_prefix(self):
        clock = Clock()
        rec = RecencyTracker(half_life=1000, time_fn=clock)
        rec.record("data mining")
        rec.record("data science")
        rec.record("london")
        matches = {q for q, _ in rec.matching_prefix("data")}
        self.assertEqual(matches, {"data mining", "data science"})
        self.assertEqual(rec.matching_prefix("zzz"), [])


class TestBlend(unittest.TestCase):
    def test_recency_promotes_within_pool(self):
        candidates = [("alpha", 1_000_000), ("beta", 10_000)]
        # With no recency, alpha (huge count) wins.
        ranked = blend(candidates, {}, history_weight=0.4, recency_weight=0.6, limit=2)
        self.assertEqual(ranked[0][0], "alpha")
        # Give beta a strong recency signal; it should overtake.
        ranked = blend(candidates, {"beta": 50.0}, history_weight=0.4, recency_weight=0.6, limit=2)
        self.assertEqual(ranked[0][0], "beta")

    def test_blend_empty(self):
        self.assertEqual(blend([], {}, 0.5, 0.5, 10), [])

    def test_limit_respected(self):
        cands = [(f"q{i}", 100 - i) for i in range(20)]
        ranked = blend(cands, {}, 0.5, 0.5, 5)
        self.assertEqual(len(ranked), 5)


if __name__ == "__main__":
    unittest.main()
