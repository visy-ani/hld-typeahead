"""SuggestionService — the orchestration layer the API calls into.

It owns the read path (suggest), the write path (search submission), trending,
cache debugging, and metrics aggregation, wiring together the trie, distributed
cache, recency tracker, batch writer, and primary store.

Read path for /suggest:
    normalize prefix -> cache lookup (consistent-hash routed)
        HIT  -> return cached result
        MISS -> trie candidates -> rank (basic or trending) -> cache.set -> return

Write path for /search:
    normalize query -> batch_writer.submit (WAL + buffer + recency) -> "Searched"
"""

from __future__ import annotations

import re
import time
from typing import Any

from .batch_writer import BatchWriter
from .cache import DistributedCache
from .config import Settings
from .metrics import Metrics
from .ranking import RecencyTracker, blend
from .storage import Storage
from .trie import Trie

VALID_MODES = ("basic", "trending")
_WS = re.compile(r"\s+")


def normalize(text: str | None) -> str:
    """Canonicalise a prefix or query.

    Lowercases (so mixed-case input matches), trims, and collapses internal
    whitespace to single spaces. Strips characters that would corrupt the
    line-based WAL. ``None``/empty -> "".
    """
    if not text:
        return ""
    text = text.replace("\n", " ").replace("\t", " ").replace("\r", " ")
    return _WS.sub(" ", text).strip().lower()


class SuggestionService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.storage = Storage(settings.db_path)
        self.trie = Trie(capacity=settings.trie_node_capacity)
        self.cache = DistributedCache(
            node_count=settings.cache_nodes,
            vnodes=settings.cache_vnodes,
            ttl=settings.cache_ttl_seconds,
            capacity_per_node=settings.cache_capacity_per_node,
        )
        self.recency = RecencyTracker(
            half_life=settings.recency_half_life,
            capacity=settings.trending_capacity,
        )
        self.metrics = Metrics(latency_window=settings.latency_window)
        self.batch_writer = BatchWriter(
            self.storage,
            self.trie,
            self.cache,
            self.recency,
            self.metrics,
            max_size=settings.batch_max_size,
            flush_interval=settings.batch_flush_interval,
            increment=settings.new_query_initial_count,
            wal_enabled=settings.wal_enabled,
            wal_path=settings.wal_path,
        )

    # -- startup / shutdown -------------------------------------------------
    def load_index(self) -> int:
        """Build the in-memory trie from the primary store. Returns row count."""
        self.trie.bulk_load(self.storage.load_all())
        return len(self.trie)

    def recover(self) -> int:
        return self.batch_writer.recover()

    def start_background(self) -> None:
        self.batch_writer.start()

    async def shutdown(self) -> None:
        await self.batch_writer.stop()
        self.storage.close()

    # -- read path ----------------------------------------------------------
    def suggest(self, raw_prefix: str | None, mode: str | None = None, limit: int | None = None) -> dict[str, Any]:
        t0 = time.perf_counter()
        prefix = normalize(raw_prefix)
        mode = mode if mode in VALID_MODES else self.settings.default_ranking_mode
        limit = limit or self.settings.suggest_limit

        if not prefix:
            # Empty/missing input: nothing to suggest. Handled gracefully, and we
            # don't pay for a cache or trie lookup.
            latency_ms = (time.perf_counter() - t0) * 1000
            self.metrics.record_suggest(latency_ms, mode)
            return {
                "prefix": "",
                "mode": mode,
                "count": 0,
                "suggestions": [],
                "cache": "bypass",
                "node": None,
                "latency_ms": round(latency_ms, 4),
            }

        # The cache stores the FULL ranked candidate pool for (prefix, mode),
        # independent of `limit`. We slice to the requested limit on the way out,
        # so different limits for the same prefix are all served correctly from a
        # single cache entry (and the cache key needn't include limit).
        hit, cached = self.cache.get(prefix, mode)
        if hit:
            full = cached
            cache_status = "hit"
        else:
            full = self._rank(prefix, mode)
            self.cache.set(prefix, mode, full)
            cache_status = "miss"
        suggestions = full[:limit]

        latency_ms = (time.perf_counter() - t0) * 1000
        self.metrics.record_suggest(latency_ms, mode)
        return {
            "prefix": prefix,
            "mode": mode,
            "count": len(suggestions),
            "suggestions": suggestions,
            "cache": cache_status,
            "node": self.cache.node_for(prefix, mode).node_id,
            "latency_ms": round(latency_ms, 4),
        }

    def _rank(self, prefix: str, mode: str) -> list[dict[str, Any]]:
        """Rank the FULL candidate pool for a prefix (up to the trie's node
        capacity). The caller slices to the requested limit; caching the full
        pool means any limit is served from one cache entry."""
        pool = self.settings.trie_node_capacity
        if mode == "trending":
            candidates = self.trie.candidates(prefix)  # all-time top-K for the prefix
            seen = {q for q, _ in candidates}
            # Merge in queries that are trending *right now* under this prefix even
            # if they're outside the all-time top-K — otherwise a surging query
            # could never be surfaced by recency. (See DESIGN.md, trending.)
            for q, _score in self.recency.matching_prefix(prefix):
                if q not in seen:
                    candidates.append((q, self.trie.get_count(q) or 0))
                    seen.add(q)
            rec = self.recency.scores_for([q for q, _ in candidates])
            ranked = blend(
                candidates,
                rec,
                history_weight=self.settings.history_weight,
                recency_weight=self.settings.recency_weight,
                limit=pool,
            )
            return [
                {"query": q, "count": c, "score": round(s, 6), "recency": round(rec.get(q, 0.0), 4)}
                for q, c, s in ranked
            ]
        # basic: pure all-time popularity
        return [
            {"query": q, "count": c, "score": float(c)}
            for q, c in self.trie.candidates(prefix)
        ]

    # -- write path ---------------------------------------------------------
    def search(self, raw_query: str | None) -> dict[str, Any]:
        query = normalize(raw_query)
        if not query:
            return {"message": "Searched", "query": "", "recorded": False}
        self.batch_writer.submit(query)
        return {"message": "Searched", "query": query, "recorded": True}

    # -- trending -----------------------------------------------------------
    def trending(self, n: int = 10) -> list[dict[str, Any]]:
        out = []
        for q, score in self.recency.top(n):
            out.append({
                "query": q,
                "recency_score": round(score, 4),
                "count": self.trie.get_count(q) or 0,
            })
        return out

    # -- cache debug --------------------------------------------------------
    def cache_debug(self, raw_prefix: str | None, mode: str | None = None) -> dict[str, Any]:
        prefix = normalize(raw_prefix)
        mode = mode if mode in VALID_MODES else self.settings.default_ranking_mode
        info = self.cache.debug(prefix, mode)
        node = self.cache.nodes[info.placement.node]
        return {
            "prefix": prefix,
            "mode": mode,
            "cache_key": info.cache_key,
            "key_hash": info.placement.key_hash,
            "responsible_node": info.placement.node,
            "virtual_node": info.placement.vnode_label,
            "virtual_node_hash": info.placement.vnode_hash,
            "result": "hit" if info.present else "miss",
            "ttl_remaining_s": round(info.ttl_remaining, 3) if info.ttl_remaining is not None else None,
            "node_size": node.size,
            "node_stats": vars(node.stats),
            "ring": {
                "nodes": info.ring_nodes,
                "total_points": info.ring_points,
                "vnodes_per_node": self.settings.cache_vnodes,
            },
        }

    def ring_distribution(self, sample: int = 5000) -> dict[str, Any]:
        """Show how evenly the ring spreads keys (consistent-hashing evidence)."""
        keys = [f"prefix-{i}" for i in range(sample)]
        dist = self.cache.ring.distribution(keys)
        ideal = sample / max(len(dist), 1)
        spread = {n: round(c / ideal, 3) for n, c in dist.items()}  # 1.0 == perfectly even
        return {"sample": sample, "counts": dist, "load_factor_vs_ideal": spread}

    # -- metrics ------------------------------------------------------------
    def metrics_summary(self) -> dict[str, Any]:
        return {
            "requests": self.metrics.summary(),
            "cache": self.cache.stats(),
            "storage": self.storage.stats(),
            "batch_writer": self.batch_writer.stats(),
            "index": {
                "queries": len(self.trie),
                "trie_nodes": self.trie.node_count,
                "trending_tracked": len(self.recency),
            },
        }
