"""Consistent hashing ring.

Why consistent hashing?
-----------------------
We shard the suggestion cache across N logical nodes. The naive way to pick a
node for a key is ``hash(key) % N``. The problem: change N (add/remove a node)
and *almost every* key remaps, so the whole cache is invalidated at once.

Consistent hashing places both nodes and keys on a fixed circular keyspace
(0 .. 2^32-1). A key is owned by the first node found walking clockwise from the
key's position. Adding or removing a node only remaps the keys that fall in that
node's arc — on average K/N keys — instead of all of them.

Virtual nodes (replicas)
------------------------
A node placed once on the ring grabs one contiguous arc, which can be uneven. We
instead place each physical node at ``vnodes`` pseudo-random positions. The arcs
interleave, so load evens out and removing a node spreads its keys across many
survivors rather than dumping them all on its single clockwise neighbour.

The lookup is O(log(total_points)) via binary search over the sorted ring.
"""

from __future__ import annotations

import bisect
import hashlib
from dataclasses import dataclass


def _hash(key: str) -> int:
    """Map a string to a point on the 32-bit ring.

    md5 is used purely as a fast, well-distributed hash (not for security). We
    take the first 8 hex digits => 32 bits, which is plenty of resolution for a
    handful of nodes with ~150 replicas each.
    """
    digest = hashlib.md5(key.encode("utf-8")).hexdigest()
    return int(digest[:8], 16)


@dataclass(frozen=True)
class Placement:
    """Result of routing a key — everything /cache/debug needs to explain itself."""

    key: str
    key_hash: int
    node: str
    vnode_label: str
    vnode_hash: int


class ConsistentHashRing:
    """A hash ring with configurable virtual nodes per physical node."""

    def __init__(self, nodes: list[str] | None = None, vnodes: int = 150):
        self._vnodes = vnodes
        # Sorted parallel arrays: _ring_hashes[i] -> _ring_nodes[i].
        # Kept sorted by hash so we can bisect for the clockwise-nearest node.
        self._ring_hashes: list[int] = []
        self._ring_nodes: list[str] = []
        # hash -> the "node#replica" label, for debugging/visualisation.
        self._vnode_labels: dict[int, str] = {}
        self._nodes: set[str] = set()
        for node in nodes or []:
            self.add_node(node)

    # -- mutation -----------------------------------------------------------
    def add_node(self, node: str) -> None:
        if node in self._nodes:
            return
        self._nodes.add(node)
        for replica in range(self._vnodes):
            label = f"{node}#{replica}"
            h = _hash(label)
            # On the astronomically rare hash collision, nudge with a suffix so
            # we never silently drop a replica.
            while h in self._vnode_labels:
                label = label + "~"
                h = _hash(label)
            idx = bisect.bisect_left(self._ring_hashes, h)
            self._ring_hashes.insert(idx, h)
            self._ring_nodes.insert(idx, node)
            self._vnode_labels[h] = label

    def remove_node(self, node: str) -> None:
        if node not in self._nodes:
            return
        self._nodes.discard(node)
        keep_hashes: list[int] = []
        keep_nodes: list[str] = []
        for h, n in zip(self._ring_hashes, self._ring_nodes):
            if n == node:
                self._vnode_labels.pop(h, None)
            else:
                keep_hashes.append(h)
                keep_nodes.append(n)
        self._ring_hashes = keep_hashes
        self._ring_nodes = keep_nodes

    # -- routing ------------------------------------------------------------
    def get_node(self, key: str) -> str:
        """Return the node id that owns ``key``."""
        return self.locate(key).node

    def locate(self, key: str) -> Placement:
        """Route ``key`` and return full placement detail (for /cache/debug)."""
        if not self._ring_hashes:
            raise RuntimeError("consistent hash ring is empty — no cache nodes registered")
        h = _hash(key)
        # First ring point clockwise from h; wrap to index 0 if we fall off the end.
        idx = bisect.bisect_right(self._ring_hashes, h)
        if idx == len(self._ring_hashes):
            idx = 0
        vnode_hash = self._ring_hashes[idx]
        return Placement(
            key=key,
            key_hash=h,
            node=self._ring_nodes[idx],
            vnode_label=self._vnode_labels[vnode_hash],
            vnode_hash=vnode_hash,
        )

    # -- introspection ------------------------------------------------------
    @property
    def nodes(self) -> list[str]:
        return sorted(self._nodes)

    @property
    def ring_size(self) -> int:
        """Total number of points on the ring (nodes * vnodes)."""
        return len(self._ring_hashes)

    def distribution(self, sample_keys: list[str]) -> dict[str, int]:
        """Count how many of ``sample_keys`` land on each node.

        Used by tests and the README to demonstrate that virtual nodes give a
        roughly even spread, and by /cache/debug to show ring balance.
        """
        counts = {n: 0 for n in self._nodes}
        for k in sample_keys:
            counts[self.get_node(k)] += 1
        return counts
