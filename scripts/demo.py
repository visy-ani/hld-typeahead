"""Self-contained evidence/log generator for the viva and submission.

Runs entirely in-process against a small, deterministic dataset (no server, no
network) and prints clearly-labelled logs that demonstrate the three graded
behaviours:

  1. Consistent hashing  — even key distribution across nodes, and that removing
     a node remaps only ~1/N of keys (not ~all, as `hash % N` would).
  2. Basic vs. trending  — the same prefix ranked by all-time count, then by
     recency after a burst, showing a tail query promoted to #1.
  3. Batch writes        — many searches collapsing into few DB transactions.

Usage:
    python -m scripts.demo
"""

from __future__ import annotations

import asyncio
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import Settings                       # noqa: E402
from app.consistent_hash import ConsistentHashRing    # noqa: E402
from app.service import SuggestionService             # noqa: E402
from app.storage import Storage                        # noqa: E402

LINE = "=" * 70

# A tiny, readable dataset so the output is easy to reason about.
SEED = [
    ("apple", 1_000_000), ("apple watch", 420_000), ("apple tv", 180_000),
    ("application", 90_000), ("apply", 60_000), ("apple pie recipe", 12_000),
    ("banana", 75_000), ("banana bread", 30_000),
    ("python", 800_000), ("python tutorial", 95_000), ("python pandas", 40_000),
]


def demo_consistent_hashing() -> None:
    print(f"\n{LINE}\n1. CONSISTENT HASHING\n{LINE}")
    nodes = [f"cache-{i}" for i in range(5)]
    ring = ConsistentHashRing(nodes, vnodes=150)
    keys = [f"prefix-{i}" for i in range(50_000)]

    dist = ring.distribution(keys)
    ideal = len(keys) / len(nodes)
    print(f"Routing {len(keys):,} keys across {len(nodes)} nodes "
          f"({ring.ring_size} ring points = {len(nodes)}x150 virtual nodes):")
    for n in sorted(dist):
        print(f"  {n}: {dist[n]:>6,} keys   load factor {dist[n]/ideal:.3f}  (1.000 = perfectly even)")

    before = {k: ring.get_node(k) for k in keys}
    ring.remove_node("cache-2")
    moved = sum(1 for k in keys if ring.get_node(k) != before[k])
    print(f"\nRemoving 1 of 5 nodes remapped {moved:,}/{len(keys):,} keys "
          f"= {moved/len(keys)*100:.1f}%")
    print(f"  consistent hashing  : ~1/5 = 20%  (only keys that lived on the removed node)")
    print(f"  naive `hash % N`    : ~80%        (almost every key changes owner)")
    # Routing detail for one key (what /cache/debug shows):
    p = ring.locate("iphone")
    print(f"\nExample route: key 'iphone' -> {p.node} (virtual node {p.vnode_label}, hash {p.key_hash})")


def demo_ranking(svc: SuggestionService) -> None:
    print(f"\n{LINE}\n2. BASIC vs. TRENDING RANKING\n{LINE}")

    def show(mode):
        rows = svc.suggest("apple", mode=mode)["suggestions"]
        return [(r["query"], r["count"], r.get("recency", 0)) for r in rows[:5]]

    print("Prefix 'apple' — BEFORE any recent activity:")
    print(f"  basic   : {[q for q,_,_ in show('basic')]}")
    print(f"  trending: {[q for q,_,_ in show('trending')]}")

    print("\nNow burst-search the tail query 'apple pie recipe' 80x, then flush...")
    for _ in range(80):
        svc.search("apple pie recipe")
    asyncio.run(svc.batch_writer.flush())

    print("\nPrefix 'apple' — AFTER the burst:")
    b = show("basic")
    t = show("trending")
    print(f"  basic    (#1 = {b[0][0]!r}): {[q for q,_,_ in b]}")
    print(f"             ^ still count-ranked; 'apple' ({b[0][1]:,}) stays on top")
    print(f"  trending (#1 = {t[0][0]!r}): {[q for q,_,_ in t]}")
    print(f"             ^ recency promotes 'apple pie recipe' (recency {t[0][2]}) despite a far lower count")


def demo_batch_writes(svc: SuggestionService) -> None:
    print(f"\n{LINE}\n3. BATCH WRITES (write reduction)\n{LINE}")
    bw = svc.batch_writer
    before = bw.stats()
    storage_before = svc.storage.stats()["write_transactions"]
    distinct = ["python", "python tutorial", "banana", "apple", "apply"]
    total = 1000
    print(f"Submitting {total:,} searches over {len(distinct)} distinct queries, "
          f"flushing every {svc.settings.batch_flush_interval}s or {svc.settings.batch_max_size} queries...")
    for i in range(total):
        svc.search(distinct[i % len(distinct)])
    asyncio.run(svc.batch_writer.flush())
    after = bw.stats()
    txns = svc.storage.stats()["write_transactions"] - storage_before
    searches = after["searches_received"] - before["searches_received"]
    upserts = after["db_upserts"] - before["db_upserts"]
    print(f"\n  searches submitted : {searches:,}")
    print(f"  DB upserts         : {upserts:,}   (in {txns} transaction(s))")
    print(f"  write reduction    : {searches/upserts:.1f}x  "
          f"({(1-upserts/searches)*100:.1f}% of per-request writes avoided)")
    print(f"  (without batching this would be {searches:,} writes in {searches:,} transactions)")


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        db = str(Path(tmp) / "demo.db")
        Storage(db).bulk_replace(SEED)
        settings = Settings(
            db_path=db, wal_path=str(Path(tmp) / "demo.wal"),
            batch_flush_interval=999, batch_max_size=100_000,
            recency_half_life=3600, trie_node_capacity=25,
        )
        svc = SuggestionService(settings)
        svc.load_index()
        svc.recover()
        print("Search Typeahead — demonstration logs")
        print(f"(in-process, {len(SEED)} seed queries, deterministic)")
        demo_consistent_hashing()
        demo_ranking(svc)
        demo_batch_writes(svc)
        asyncio.run(svc.shutdown())
        print(f"\n{LINE}\nDone. See DESIGN.md for the reasoning behind each behaviour.\n{LINE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
