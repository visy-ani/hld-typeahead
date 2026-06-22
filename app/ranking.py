"""Ranking: all-time popularity (basic) and recency-aware trending (enhanced).

Basic mode just sorts by stored count — historically popular queries first.

Trending mode blends that with how *recently* a query is being searched, so a
query that is surging right now can outrank an all-time favourite for the same
prefix. The four questions the assignment asks us to answer:

1. How recent searches are tracked
   ----------------------------------
   A ``RecencyTracker`` keeps, per query, a single time-decayed counter. Each
   search adds 1.0 to that query's score. The score continuously decays with an
   exponential half-life H: after H seconds, a past search contributes half as
   much; after 2H, a quarter; and so on. We store only (score, last_update) and
   apply the decay lazily on read/write, so it is O(1) per search — no
   per-minute buckets or background sweeps.

2. How recent activity affects ranking
   ------------------------------------
   In trending mode the final score is a weighted blend of a normalised,
   log-compressed historical count and the normalised decayed recency score:

       score = w_history * norm(log1p(count)) + w_recency * norm(recency)

   log1p compresses the enormous historical range (counts span many orders of
   magnitude) so a genuine recency surge can compete instead of being buried.

3. How we avoid permanently over-ranking a brief spike
   ----------------------------------------------------
   Decay is the mechanism: a query that spikes then goes quiet sees its recency
   score halve every H seconds and fade back to ~0, so it cannot stay near the
   top once the burst ends. (A pure cumulative counter could never recover from
   a spike — that's exactly the failure mode decay prevents.)

4. Trade-offs (freshness vs latency vs complexity)
   ------------------------------------------------
   Short H = very fresh but jumpy and cache-churny; long H = smoother but
   slower to react. The decay math is a handful of float ops, so latency cost is
   negligible; the real cost is cache freshness, which we bound with TTL +
   targeted invalidation (see DESIGN.md).
"""

from __future__ import annotations

import math
from typing import Callable


# 0.5 ** (dt / H): fraction of a score that survives after dt seconds.
def _decay_factor(dt: float, half_life: float) -> float:
    if dt <= 0:
        return 1.0
    if half_life <= 0:
        return 0.0
    return 0.5 ** (dt / half_life)


class _State:
    __slots__ = ("score", "updated")

    def __init__(self, score: float, updated: float):
        self.score = score
        self.updated = updated


class RecencyTracker:
    """Per-query exponentially-decayed activity counter."""

    def __init__(self, half_life: float, capacity: int = 50000, time_fn: Callable[[], float] | None = None):
        import time as _time

        self.half_life = half_life
        self.capacity = capacity
        self._now = time_fn or _time.time
        self._state: dict[str, _State] = {}

    def record(self, query: str, weight: float = 1.0, now: float | None = None) -> None:
        """Register a search for ``query`` (adds ``weight`` to its decayed score)."""
        now = self._now() if now is None else now
        st = self._state.get(query)
        if st is None:
            self._state[query] = _State(weight, now)
            if len(self._state) > self.capacity:
                self._prune()
        else:
            # Decay the existing score up to `now`, then add the new weight.
            st.score = st.score * _decay_factor(now - st.updated, self.half_life) + weight
            st.updated = now

    def score(self, query: str, now: float | None = None) -> float:
        """Current decayed recency score for ``query`` (0 if never seen)."""
        st = self._state.get(query)
        if st is None:
            return 0.0
        now = self._now() if now is None else now
        return st.score * _decay_factor(now - st.updated, self.half_life)

    def scores_for(self, queries: list[str], now: float | None = None) -> dict[str, float]:
        now = self._now() if now is None else now
        return {q: self.score(q, now) for q in queries}

    def top(self, n: int, now: float | None = None) -> list[tuple[str, float]]:
        """Top ``n`` trending queries by current decayed score."""
        now = self._now() if now is None else now
        scored = [
            (q, st.score * _decay_factor(now - st.updated, self.half_life))
            for q, st in self._state.items()
        ]
        scored.sort(key=lambda e: (-e[1], e[0]))
        return scored[:n]

    def matching_prefix(
        self, prefix: str, now: float | None = None, min_score: float = 1e-3, limit: int = 50
    ) -> list[tuple[str, float]]:
        """Currently-trending queries that start with ``prefix``.

        This is what lets a *surging but not historically popular* query appear
        in trending suggestions even when it's outside the prefix's all-time
        top-K (e.g. a brand-new query everyone suddenly searches). One O(tracked)
        scan, paid only on a trending cache miss; the tracked set is bounded by
        ``capacity``.
        """
        now = self._now() if now is None else now
        out = []
        for q, st in self._state.items():
            if q.startswith(prefix):
                s = st.score * _decay_factor(now - st.updated, self.half_life)
                if s > min_score:
                    out.append((q, s))
        out.sort(key=lambda e: (-e[1], e[0]))
        return out[:limit]

    def _prune(self) -> None:
        """Drop the lowest-scoring entries back down to capacity.

        Keeps the tracker bounded; only the freshest/strongest queries survive,
        which are exactly the ones that can ever appear in trending results.
        """
        now = self._now()
        scored = sorted(
            self._state.items(),
            key=lambda kv: kv[1].score * _decay_factor(now - kv[1].updated, self.half_life),
            reverse=True,
        )
        self._state = dict(scored[: self.capacity])

    def __len__(self) -> int:
        return len(self._state)


def blend(
    candidates: list[tuple[str, int]],
    recency_scores: dict[str, float],
    history_weight: float,
    recency_weight: float,
    limit: int,
) -> list[tuple[str, int, float]]:
    """Re-rank a prefix's candidate pool with the recency-aware blend.

    Normalisation is done *within the candidate pool* so the two signals are on a
    comparable 0..1 scale before weighting. Returns (query, count, score) sorted
    by blended score descending.
    """
    if not candidates:
        return []
    max_log = max(math.log1p(c) for _q, c in candidates) or 1.0
    max_rec = max((recency_scores.get(q, 0.0) for q, _c in candidates), default=0.0) or 1.0
    ranked = []
    for q, c in candidates:
        h = math.log1p(c) / max_log
        r = recency_scores.get(q, 0.0) / max_rec
        ranked.append((q, c, history_weight * h + recency_weight * r))
    ranked.sort(key=lambda e: (-e[2], -e[1], e[0]))
    return ranked[:limit]
