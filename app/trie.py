"""Prefix index: a Trie that caches the top-K queries at every node.

The lookup problem
------------------
For a prefix p we need the 10 highest-count queries that start with p. Scanning
all queries per keystroke is O(N) — far too slow at 100k+ queries and one
request per typed character. A plain trie gives us O(len(p)) to reach the prefix
node, but then we'd still have to walk its whole subtree to find the top 10.

The fix: **materialise the top-K at each node.** Every node stores the K
highest-count queries living anywhere in its subtree, pre-sorted. A lookup is
then: walk len(p) edges to the prefix node, slice its list. O(len(p)) total,
independent of dataset size.

Two ways the top-K lists are maintained
---------------------------------------
1. ``bulk_load`` — one-time ingestion. Insert all terminals, then a single
   post-order pass merges each node's children lists into its own top-K. Each
   node's true top-K is a subset of the union of its children's top-K (proof in
   DESIGN.md), so the merge is exact.
2. ``upsert`` — live updates from the batch writer. Counts only ever increase,
   so the changed query is re-placed along its own prefix path; no other query's
   rank can move. This keeps every ancestor's top-K exact under increments.

Memory note: total stored entries = Σ over nodes of min(subtree_size, K). Deep
nodes have tiny subtrees, so this is bounded by Σ len(query), not nodes * K.
"""

from __future__ import annotations

import sys
from typing import Iterable, Iterator


class _Node:
    __slots__ = ("children", "top", "word", "count")

    def __init__(self) -> None:
        self.children: dict[str, _Node] = {}
        # top: list of (query, count) sorted by count desc, then query asc.
        self.top: list[tuple[str, int]] = []
        # The query terminating exactly at this node (None if this is an
        # internal-only node) and its count.
        self.word: str | None = None
        self.count: int = 0


def _rank_key(entry: tuple[str, int]):
    # Sort by count descending; break ties by query ascending so output is
    # deterministic (important for reproducible tests and demos).
    return (-entry[1], entry[0])


class Trie:
    """Prefix index with per-node materialised top-K."""

    def __init__(self, capacity: int = 25):
        # capacity = how many candidates each node retains. The API returns 10;
        # we keep more so a recency re-ranker can promote trending queries that
        # are outside the all-time top 10 for the prefix.
        self.capacity = capacity
        self._root = _Node()
        self._size = 0          # number of distinct queries
        self._nodes = 1         # number of trie nodes (for metrics)

    # -- size / introspection ----------------------------------------------
    def __len__(self) -> int:
        return self._size

    @property
    def node_count(self) -> int:
        return self._nodes

    # -- bulk ingestion -----------------------------------------------------
    def bulk_load(self, items: Iterable[tuple[str, int]]) -> None:
        """Load many (query, count) pairs efficiently.

        Safe to call once on an empty trie. Existing terminals for a repeated
        query are overwritten with the latest count.
        """
        root = self._root
        for query, count in items:
            if not query:
                continue
            node = root
            for ch in query:
                nxt = node.children.get(ch)
                if nxt is None:
                    nxt = _Node()
                    node.children[ch] = nxt
                    self._nodes += 1
                node = nxt
            if node.word is None:
                self._size += 1
            node.word = query
            node.count = count
        # Bump recursion limit: depth is bounded by the longest query, but we
        # raise it defensively so unusually long entries can't crash the build.
        old_limit = sys.getrecursionlimit()
        sys.setrecursionlimit(max(old_limit, 100000))
        try:
            self._build_top(root)
        finally:
            sys.setrecursionlimit(old_limit)

    def _build_top(self, node: _Node) -> None:
        candidates: list[tuple[str, int]] = []
        if node.word is not None:
            candidates.append((node.word, node.count))
        for child in node.children.values():
            self._build_top(child)
            candidates.extend(child.top)
        candidates.sort(key=_rank_key)
        node.top = candidates[: self.capacity]

    # -- live updates -------------------------------------------------------
    def upsert(self, query: str, count: int) -> None:
        """Set ``query``'s count to ``count`` and refresh every ancestor's top-K.

        Correct for monotonically increasing counts (the only kind the batch
        writer produces): the changed query is the only one whose rank can move,
        and it lies on this exact path, so re-placing it at each node on the path
        keeps all top-K lists exact.
        """
        if not query:
            return
        node = self._root
        path = [node]
        for ch in query:
            nxt = node.children.get(ch)
            if nxt is None:
                nxt = _Node()
                node.children[ch] = nxt
                self._nodes += 1
            node = nxt
            path.append(node)
        if node.word is None:
            self._size += 1
        node.word = query
        node.count = count
        for n in path:
            self._upsert_node(n, query, count)

    def _upsert_node(self, node: _Node, query: str, count: int) -> None:
        top = node.top
        # Already present? Update in place then re-sort (lists are tiny).
        for i, (q, _c) in enumerate(top):
            if q == query:
                top[i] = (query, count)
                top.sort(key=_rank_key)
                return
        # Not present: insert if there's room or it beats the current worst.
        if len(top) < self.capacity:
            top.append((query, count))
            top.sort(key=_rank_key)
        else:
            worst = top[-1]
            if (-count, query) < _rank_key(worst):
                top[-1] = (query, count)
                top.sort(key=_rank_key)

    # -- lookup -------------------------------------------------------------
    def _node_for_prefix(self, prefix: str) -> _Node | None:
        node = self._root
        for ch in prefix:
            node = node.children.get(ch)
            if node is None:
                return None
        return node

    def candidates(self, prefix: str) -> list[tuple[str, int]]:
        """Full candidate pool (up to ``capacity``) for a prefix.

        Returns a fresh list so callers can re-rank without mutating the index.
        Empty prefix returns the global top-K (useful as a sensible default).
        """
        node = self._node_for_prefix(prefix)
        if node is None:
            return []
        return list(node.top)

    def top(self, prefix: str, limit: int = 10) -> list[tuple[str, int]]:
        """Top ``limit`` suggestions for a prefix, sorted by count desc."""
        return self.candidates(prefix)[:limit]

    def contains(self, query: str) -> bool:
        node = self._node_for_prefix(query)
        return node is not None and node.word is not None

    def get_count(self, query: str) -> int | None:
        node = self._node_for_prefix(query)
        if node is None or node.word is None:
            return None
        return node.count

    def iter_words(self) -> Iterator[tuple[str, int]]:
        """Yield every (query, count). Used by tests; not on any hot path."""
        stack = [self._root]
        while stack:
            n = stack.pop()
            if n.word is not None:
                yield (n.word, n.count)
            stack.extend(n.children.values())
