"""Batch writer: aggregation + write-reduction, post-flush trie refresh and cache
invalidation, and crash recovery from the write-ahead log."""

import asyncio
import os
import tempfile
import unittest

from app.batch_writer import BatchWriter
from app.cache import DistributedCache
from app.metrics import Metrics
from app.ranking import RecencyTracker
from app.storage import Storage
from app.trie import Trie


def make_writer(tmp, max_size=10_000, initial=None):
    db = os.path.join(tmp, "t.db")
    wal = os.path.join(tmp, "buffer.wal")
    storage = Storage(db)
    if initial:
        storage.bulk_replace(initial)
    trie = Trie(capacity=10)
    trie.bulk_load(storage.load_all())
    cache = DistributedCache(node_count=3, vnodes=50, ttl=1000, capacity_per_node=1000)
    recency = RecencyTracker(half_life=1000)
    metrics = Metrics()
    bw = BatchWriter(
        storage, trie, cache, recency, metrics,
        max_size=max_size, flush_interval=999, increment=1,
        wal_enabled=True, wal_path=wal,
    )
    return bw, storage, trie, cache, wal


class TestBatchWriter(unittest.TestCase):
    def test_aggregation_and_write_reduction(self):
        with tempfile.TemporaryDirectory() as tmp:
            bw, storage, trie, cache, wal = make_writer(tmp)
            bw.recover()
            for _ in range(5):
                bw.submit("apple")
            for _ in range(3):
                bw.submit("banana")
            self.assertEqual(bw.searches_received, 8)
            written = asyncio.run(bw.flush())
            # 8 searches collapsed into 2 upserts in 1 transaction.
            self.assertEqual(written, 2)
            self.assertEqual(storage.get_count("apple"), 5)
            self.assertEqual(storage.get_count("banana"), 3)
            self.assertEqual(storage.write_transactions, 1)
            stats = bw.stats()
            self.assertEqual(stats["db_upserts"], 2)
            self.assertEqual(stats["write_reduction_ratio"], 4.0)  # 8 / 2
            asyncio.run(bw.stop())

    def test_trie_updated_after_flush(self):
        with tempfile.TemporaryDirectory() as tmp:
            bw, storage, trie, cache, wal = make_writer(tmp, initial=[("cat", 100)])
            bw.recover()
            for _ in range(10):
                bw.submit("cat")
            bw.submit("car")
            asyncio.run(bw.flush())
            self.assertEqual(trie.get_count("cat"), 110)
            self.assertEqual(trie.get_count("car"), 1)
            # the index reflects it in suggestions
            self.assertIn("cat", [q for q, _ in trie.top("ca", 10)])
            asyncio.run(bw.stop())

    def test_cache_invalidated_on_flush(self):
        with tempfile.TemporaryDirectory() as tmp:
            bw, storage, trie, cache, wal = make_writer(tmp, initial=[("abc", 5)])
            bw.recover()
            # Warm the cache for prefixes that 'abc' affects.
            cache.set("a", "basic", ["stale"])
            cache.set("ab", "trending", ["stale"])
            self.assertTrue(cache.get("a", "basic")[0])
            bw.submit("abc")
            asyncio.run(bw.flush())
            # Flushing 'abc' must invalidate its prefixes in all modes.
            self.assertFalse(cache.get("a", "basic")[0])
            self.assertFalse(cache.get("ab", "trending")[0])
            asyncio.run(bw.stop())

    def test_flush_with_wal_disabled(self):
        """With the WAL off, flush applies straight from the in-memory buffer."""
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(os.path.join(tmp, "t.db"))
            trie = Trie(capacity=10)
            cache = DistributedCache(3, 50, 1000, 1000)
            bw = BatchWriter(
                storage, trie, cache, RecencyTracker(half_life=1000), Metrics(),
                max_size=10_000, flush_interval=999, wal_enabled=False,
                wal_path=os.path.join(tmp, "buffer.wal"),
            )
            bw.recover()
            for _ in range(4):
                bw.submit("kiwi")
            self.assertEqual(asyncio.run(bw.flush()), 1)
            self.assertEqual(storage.get_count("kiwi"), 4)
            self.assertEqual(trie.get_count("kiwi"), 4)
            # no WAL files were created
            self.assertEqual(__import__("glob").glob(os.path.join(tmp, "buffer.wal*")), [])
            asyncio.run(bw.stop())

    def test_empty_flush_is_noop(self):
        with tempfile.TemporaryDirectory() as tmp:
            bw, storage, trie, cache, wal = make_writer(tmp)
            bw.recover()
            self.assertEqual(asyncio.run(bw.flush()), 0)
            asyncio.run(bw.stop())

    def test_no_loss_across_consecutive_failed_flushes(self):
        """A flush fails, more searches arrive, a second flush also fails, then
        the process crashes. Recovery must replay BOTH batches exactly once —
        no loss (segments persist) and no double-count (each datum in one
        segment, never re-buffered)."""
        with tempfile.TemporaryDirectory() as tmp:
            bw, storage, trie, cache, wal = make_writer(tmp)
            bw.recover()

            def boom(*a, **k):
                raise RuntimeError("db down")
            good = storage.apply_batch
            storage.apply_batch = boom

            bw.submit("alpha")
            with self.assertRaises(RuntimeError):
                asyncio.run(bw.flush())          # flush 1 fails -> seg.1 (alpha) kept
            bw.submit("beta")
            with self.assertRaises(RuntimeError):
                asyncio.run(bw.flush())          # flush 2 fails -> seg.2 (beta) kept
            storage.apply_batch = good
            if bw._wal_file:
                bw._wal_file.close()             # release the live WAL handle

            # Simulate crash + restart: brand-new writer recovers from the WAL.
            from app.storage import Storage as _S
            storage2 = _S(os.path.join(tmp, "t.db"))
            trie2 = Trie(capacity=10)
            trie2.bulk_load(storage2.load_all())
            bw2 = BatchWriter(
                storage2, trie2, DistributedCache(3, 50, 1000, 1000),
                RecencyTracker(half_life=1000), Metrics(),
                max_size=10_000, flush_interval=999, wal_enabled=True, wal_path=wal,
            )
            bw2.recover()
            self.assertEqual(storage2.get_count("alpha"), 1)   # exactly once
            self.assertEqual(storage2.get_count("beta"), 1)    # exactly once
            if bw2._wal_file:
                bw2._wal_file.close()
            storage.close()
            storage2.close()

    def test_commit_then_crash_does_not_double_count(self):
        """The dangerous window: a batch committed (watermark persisted) but the
        process crashed before the segment file was deleted. Recovery must NOT
        replay it — the watermark tells us it's already applied."""
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "t.db")
            wal = os.path.join(tmp, "buffer.wal")
            storage = Storage(db)
            storage.bulk_replace([("apple", 10)])
            # Simulate: seq 5 was committed (apple already +1 = 11) and watermarked,
            # but the segment file survived the crash.
            storage.apply_batch({"apple": 1}, 0.0, seq=5)
            self.assertEqual(storage.get_count("apple"), 11)
            storage.close()
            with open(wal + ".seg.5", "w") as fh:
                fh.write("apple\n")              # the un-deleted, already-applied segment

            storage = Storage(db)
            trie = Trie(capacity=10)
            trie.bulk_load(storage.load_all())
            bw = BatchWriter(
                storage, trie, DistributedCache(3, 50, 1000, 1000),
                RecencyTracker(half_life=1000), Metrics(),
                max_size=10_000, flush_interval=999, wal_enabled=True, wal_path=wal,
            )
            bw.recover()
            self.assertEqual(storage.get_count("apple"), 11)        # NOT 12
            self.assertFalse(os.path.exists(wal + ".seg.5"))        # stale segment dropped
            asyncio.run(bw.stop())

    def test_wal_crash_recovery(self):
        """Crash with un-applied work: an un-applied segment (seq > watermark)
        plus the live WAL. recover() replays both into the DB and trie."""
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "t.db")
            wal = os.path.join(tmp, "buffer.wal")
            storage = Storage(db)
            storage.bulk_replace([("x", 100)])
            storage.close()
            # An un-applied segment (seq 3, watermark still 0) plus a live WAL.
            with open(wal + ".seg.3", "w") as fh:
                fh.write("x\nx\ny\n")
            with open(wal, "w") as fh:
                fh.write("z\n")

            storage = Storage(db)
            trie = Trie(capacity=10)
            trie.bulk_load(storage.load_all())
            bw = BatchWriter(
                storage, trie, DistributedCache(3, 50, 1000, 1000),
                RecencyTracker(half_life=1000), Metrics(),
                max_size=10_000, flush_interval=999, wal_enabled=True, wal_path=wal,
            )
            replayed = bw.recover()
            self.assertEqual(replayed, 4)                   # x,x,y (seg) + z (live)
            self.assertEqual(storage.get_count("x"), 102)   # 100 + 2
            self.assertEqual(storage.get_count("y"), 1)
            self.assertEqual(storage.get_count("z"), 1)
            self.assertEqual(trie.get_count("x"), 102)      # index reconciled too
            self.assertFalse(os.path.exists(wal + ".seg.3"))
            # watermark advanced past the replayed seq
            self.assertGreaterEqual(storage.get_meta_int("last_flush_seq"), 3)
            asyncio.run(bw.stop())


if __name__ == "__main__":
    unittest.main()
