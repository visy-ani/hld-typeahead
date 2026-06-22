"""Distributed suggestion cache.

The cache sits in front of the trie so that repeated prefixes (the common case
for a popular search box) are served without touching the index at all.

Topology
--------
``DistributedCache`` owns N independent ``CacheNode`` objects. A
``ConsistentHashRing`` decides which node owns a given prefix key. Each node has
its own store, its own LRU eviction, and its own hit/miss counters — exactly as
separate cache servers would. Swapping ``CacheNode`` for a Redis-backed class
later would not change the routing layer at all.

Per-entry semantics
-------------------
* **TTL** — every entry expires after ``ttl`` seconds. This bounds staleness
  from recency-aware ranking without any explicit invalidation.
* **LRU** — when a node exceeds its capacity, the least-recently-used entry is
  evicted. An ``OrderedDict`` gives O(1) move-to-end and popitem(last=False).

A monotonic clock is injected (``time_fn``) so tests can advance time
deterministically instead of sleeping.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable

from .consistent_hash import ConsistentHashRing, Placement


@dataclass
class _Entry:
    value: Any
    expires_at: float


@dataclass
class NodeStats:
    hits: int = 0
    misses: int = 0
    sets: int = 0
    evictions: int = 0
    expirations: int = 0

    @property
    def lookups(self) -> int:
        return self.hits + self.misses

    @property
    def hit_rate(self) -> float:
        return self.hits / self.lookups if self.lookups else 0.0


class CacheNode:
    """One logical cache server: an LRU map of key -> (value, expiry)."""

    def __init__(self, node_id: str, capacity: int, ttl: float, time_fn: Callable[[], float]):
        self.node_id = node_id
        self.capacity = capacity
        self.ttl = ttl
        self._now = time_fn
        self._store: "OrderedDict[str, _Entry]" = OrderedDict()
        self.stats = NodeStats()

    def get(self, key: str) -> tuple[bool, Any]:
        """Return (hit, value). A miss covers both absent and expired keys."""
        entry = self._store.get(key)
        if entry is None:
            self.stats.misses += 1
            return False, None
        if entry.expires_at <= self._now():
            # Lazily evict expired entries on access.
            del self._store[key]
            self.stats.expirations += 1
            self.stats.misses += 1
            return False, None
        # Touch: most-recently used moves to the end.
        self._store.move_to_end(key)
        self.stats.hits += 1
        return True, entry.value

    def set(self, key: str, value: Any) -> None:
        entry = _Entry(value=value, expires_at=self._now() + self.ttl)
        if key in self._store:
            self._store[key] = entry
            self._store.move_to_end(key)
        else:
            self._store[key] = entry
            if len(self._store) > self.capacity:
                # Evict least-recently used (front of the OrderedDict).
                self._store.popitem(last=False)
                self.stats.evictions += 1
        self.stats.sets += 1

    def invalidate(self, key: str) -> bool:
        return self._store.pop(key, None) is not None

    def ttl_remaining(self, key: str) -> float | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        return max(0.0, entry.expires_at - self._now())

    def clear(self) -> None:
        self._store.clear()

    @property
    def size(self) -> int:
        return len(self._store)


@dataclass
class DebugInfo:
    """Everything GET /cache/debug reports for a prefix."""

    prefix: str
    cache_key: str
    placement: Placement
    present: bool          # currently cached (and not expired)?
    ttl_remaining: float | None
    ring_nodes: list[str]
    ring_points: int


class DistributedCache:
    """N cache nodes behind a consistent-hash ring."""

    def __init__(
        self,
        node_count: int,
        vnodes: int,
        ttl: float,
        capacity_per_node: int,
        namespace: str = "suggest",
        time_fn: Callable[[], float] | None = None,
    ):
        self._now = time_fn or time.monotonic
        self.namespace = namespace
        self._node_ids = [f"cache-{i}" for i in range(node_count)]
        self.ring = ConsistentHashRing(self._node_ids, vnodes=vnodes)
        self.nodes: dict[str, CacheNode] = {
            nid: CacheNode(nid, capacity_per_node, ttl, self._now) for nid in self._node_ids
        }

    # -- key scheme ---------------------------------------------------------
    def _key(self, prefix: str, mode: str) -> str:
        # The ranking mode is part of the key: 'basic' and 'trending' results for
        # the same prefix are different payloads and must not collide.
        return f"{self.namespace}:{mode}:{prefix}"

    def node_for(self, prefix: str, mode: str) -> CacheNode:
        return self.nodes[self.ring.get_node(self._key(prefix, mode))]

    # -- operations ---------------------------------------------------------
    def get(self, prefix: str, mode: str) -> tuple[bool, Any]:
        return self.node_for(prefix, mode).get(self._key(prefix, mode))

    def set(self, prefix: str, mode: str, value: Any) -> None:
        self.node_for(prefix, mode).set(self._key(prefix, mode), value)

    def invalidate(self, prefix: str, mode: str) -> bool:
        return self.node_for(prefix, mode).invalidate(self._key(prefix, mode))

    def invalidate_all_modes(self, prefix: str, modes: tuple[str, ...] = ("basic", "trending")) -> int:
        """Drop every cached ranking variant of a prefix (used on writes)."""
        return sum(int(self.invalidate(prefix, m)) for m in modes)

    # -- debug / introspection ---------------------------------------------
    def debug(self, prefix: str, mode: str) -> DebugInfo:
        cache_key = self._key(prefix, mode)
        placement = self.ring.locate(cache_key)
        node = self.nodes[placement.node]
        ttl_left = node.ttl_remaining(cache_key)
        return DebugInfo(
            prefix=prefix,
            cache_key=cache_key,
            placement=placement,
            present=ttl_left is not None,
            ttl_remaining=ttl_left,
            ring_nodes=self.ring.nodes,
            ring_points=self.ring.ring_size,
        )

    def stats(self) -> dict[str, Any]:
        per_node = {nid: vars(n.stats) | {"size": n.size} for nid, n in self.nodes.items()}
        total_hits = sum(n.stats.hits for n in self.nodes.values())
        total_misses = sum(n.stats.misses for n in self.nodes.values())
        lookups = total_hits + total_misses
        return {
            "nodes": per_node,
            "total_hits": total_hits,
            "total_misses": total_misses,
            "lookups": lookups,
            "hit_rate": (total_hits / lookups) if lookups else 0.0,
            "ring_points": self.ring.ring_size,
        }

    def clear(self) -> None:
        for node in self.nodes.values():
            node.clear()
