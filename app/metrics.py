"""Lightweight in-process metrics.

The rubric asks us to report latency (preferably p95), cache hit rate, and DB
read/write counts. Cache and storage track their own counters; this module adds
request counts and a latency window from which percentiles are computed.

Latencies are kept in a bounded deque (a ring buffer): O(1) to record, and
recent enough to reflect current behaviour. Percentiles are computed on demand
by sorting a snapshot — cheap because /metrics is called rarely, never on the
hot path.
"""

from __future__ import annotations

from collections import deque


def _percentile(sorted_vals: list[float], pct: float) -> float:
    """Linearly-interpolated percentile of an already-sorted list.

    Uses the rank ``pct/100 * (n-1)`` and interpolates between the two nearest
    samples, so e.g. p95 of 1..100 is 95.05, not exactly 95.
    """
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    # rank in [0, n-1]
    rank = pct / 100.0 * (len(sorted_vals) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = rank - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


class Metrics:
    def __init__(self, latency_window: int = 20000):
        self._suggest_latencies_ms: deque[float] = deque(maxlen=latency_window)
        self.suggest_requests = 0
        self.search_requests = 0
        self.suggest_by_mode: dict[str, int] = {}

    def record_suggest(self, latency_ms: float, mode: str) -> None:
        self.suggest_requests += 1
        self.suggest_by_mode[mode] = self.suggest_by_mode.get(mode, 0) + 1
        self._suggest_latencies_ms.append(latency_ms)

    def record_search(self) -> None:
        self.search_requests += 1

    def latency_summary(self) -> dict[str, float]:
        vals = sorted(self._suggest_latencies_ms)
        if not vals:
            return {"samples": 0}
        return {
            "samples": len(vals),
            "min_ms": round(vals[0], 4),
            "p50_ms": round(_percentile(vals, 50), 4),
            "p95_ms": round(_percentile(vals, 95), 4),
            "p99_ms": round(_percentile(vals, 99), 4),
            "max_ms": round(vals[-1], 4),
            "mean_ms": round(sum(vals) / len(vals), 4),
        }

    def summary(self) -> dict:
        return {
            "suggest_requests": self.suggest_requests,
            "search_requests": self.search_requests,
            "suggest_by_mode": dict(self.suggest_by_mode),
            "latency": self.latency_summary(),
        }
