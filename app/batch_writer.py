"""Batch writer — turn many search submissions into few database writes.

Why
---
Writing to SQLite on every /search would make the write path slow and put the
database under one-commit-per-request pressure. Searches are also extremely
repetitive (everyone types "iphone"). So we **buffer and aggregate**: each
submission just bumps an in-memory counter; a background task flushes the
aggregated counts to the database periodically or once the buffer is full.

Result: if "iphone" is searched 900 times and "ipad" 100 times in a flush
window, that's 1000 requests collapsed into 2 upserts in 1 transaction.

Durability — write-ahead log with exactly-once recovery
-------------------------------------------------------
The buffer is in memory, so a crash between flushes would lose those increments.
We mitigate with a **write-ahead log (WAL)** plus a **sequence watermark**:

* Every submission is appended to the live WAL *before* it is acknowledged.
* A flush **rotates** the live WAL to an immutable, sequence-numbered segment
  (`buffer.wal.seg.<n>`), clears the in-memory buffer, then applies *all*
  outstanding segments to the DB in **one transaction**. That same transaction
  also persists `meta.last_flush_seq = <max segment seq>` — so the counts and the
  "how far we got" watermark commit atomically.
* On success, the applied segment files are deleted.

This gives **exactly-once** recovery. On restart, `recover()`:
  - reads `last_flush_seq` (the high-water mark that's durably in the DB),
  - **skips and deletes** any segment whose seq ≤ watermark (already applied —
    this is the crash-after-commit-before-delete case; replaying would
    double-count, so we don't),
  - replays any segment with seq > watermark, plus the live WAL (never assigned a
    seq, so always un-applied), in one transaction under a fresh seq,
  - then deletes everything it replayed.

Because each submission's data lives in exactly one segment (we never re-buffer a
failed batch — the data stays in its segment and is retried), there is no
double-counting and no loss across process crashes, including a crash in the
middle of a flush. Verified by the tests in tests/test_batch_writer.py.

The honest residual trade-off: without per-write `fsync`, a *power loss* (not a
process crash) can lose the last few un-synced WAL lines, because the OS page
cache hasn't reached disk. `fsync`-per-write closes that at a latency cost
(config flag). This is the classic durability/throughput trade-off.
"""

from __future__ import annotations

import asyncio
import glob
import logging
import os
import time
from typing import Callable, Optional

from .cache import DistributedCache
from .metrics import Metrics
from .ranking import RecencyTracker
from .storage import Storage
from .trie import Trie

log = logging.getLogger("typeahead.batch")


class BatchWriter:
    def __init__(
        self,
        storage: Storage,
        trie: Trie,
        cache: DistributedCache,
        recency: RecencyTracker,
        metrics: Metrics,
        *,
        max_size: int,
        flush_interval: float,
        increment: int = 1,
        wal_enabled: bool = True,
        wal_path: str = "data/buffer.wal",
        fsync: bool = False,
        time_fn: Callable[[], float] | None = None,
    ):
        self._storage = storage
        self._trie = trie
        self._cache = cache
        self._recency = recency
        self._metrics = metrics
        self._max_size = max_size
        self._flush_interval = flush_interval
        self._increment = increment
        self._now = time_fn or time.time

        self._buffer: dict[str, int] = {}
        self._lock = asyncio.Lock()          # serialises flushes
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._pending_tasks: set[asyncio.Task] = set()

        # WAL
        self._wal_enabled = wal_enabled
        self._wal_path = wal_path
        self._seg_prefix = wal_path + ".seg."
        self._fsync = fsync
        self._wal_file = None
        self._seg_seq = 0          # monotonic segment id; advanced on every rotation
        self._flush_seq = 0        # last seq durably applied (DB high-water mark)

        # stats (write-reduction evidence)
        self.searches_received = 0
        self.flushes = 0
        self.rows_flushed = 0       # total (query,delta) upserts sent to the DB
        self.recovered_entries = 0

    # -- lifecycle ----------------------------------------------------------
    def recover(self) -> int:
        """Replay any WAL left over from a previous (possibly crashed) run,
        exactly once. Must be called at startup BEFORE serving and before the
        periodic flush loop. Returns the number of replayed log lines."""
        if not self._wal_enabled:
            return 0
        last_applied = self._storage.get_meta_int("last_flush_seq", 0)
        self._flush_seq = last_applied

        segments = self._list_segments()                 # sorted [(seq, path)]
        base = max([last_applied] + [s for s, _ in segments])
        deltas: dict[str, int] = {}
        replayed = 0
        to_delete: list[str] = []
        for seq, path in segments:
            if seq <= last_applied:
                # Already committed in a previous run (crash after commit, before
                # delete). Replaying would double-count, so just drop it.
                to_delete.append(path)
            else:
                replayed += self._read_into(path, deltas)
                to_delete.append(path)
        # The live WAL is never assigned a seq, so it is always un-applied.
        if os.path.exists(self._wal_path):
            replayed += self._read_into(self._wal_path, deltas)
            to_delete.append(self._wal_path)

        if deltas:
            apply_seq = base + 1
            new_counts = self._storage.apply_batch(deltas, self._now(), seq=apply_seq)
            for q, c in new_counts.items():
                self._trie.upsert(q, c)
            self._flush_seq = apply_seq
            self._seg_seq = apply_seq
            self.rows_flushed += len(deltas)
            self.flushes += 1
        else:
            self._seg_seq = base

        for path in to_delete:
            if os.path.exists(path):
                os.remove(path)
        self.recovered_entries = replayed
        self._wal_file = open(self._wal_path, "a", encoding="utf-8")
        return replayed

    def start(self) -> None:
        """Start the periodic flush loop."""
        if self._wal_enabled and self._wal_file is None:
            self._wal_file = open(self._wal_path, "a", encoding="utf-8")
        self._task = asyncio.create_task(self._run(), name="batch-writer")

    async def stop(self) -> None:
        """Stop the loop and flush whatever remains (clean shutdown)."""
        self._stop.set()
        if self._task:
            await self._task
        try:
            await self.flush()
        except Exception:
            log.exception("final flush during shutdown failed; data is safe in the WAL")
        for t in list(self._pending_tasks):
            try:
                await t
            except Exception:
                pass
        if self._wal_file is not None:
            self._wal_file.close()
            self._wal_file = None

    # -- write path ---------------------------------------------------------
    def submit(self, query: str) -> None:
        """Record a search submission. Cheap and synchronous (no DB, no await).

        Steps: append to WAL (durability) -> bump buffer (aggregation) -> update
        recency (instant trending). A size-triggered flush is scheduled if the
        buffer is full.
        """
        # 1. durability first: the search is on disk before we acknowledge it.
        if self._wal_enabled and self._wal_file is not None:
            self._wal_file.write(query + "\n")
            self._wal_file.flush()              # push to OS buffer (process-crash safe)
            if self._fsync:
                os.fsync(self._wal_file.fileno())  # power-loss safe (slower)

        # 2. aggregate: repeated queries collapse into one counter.
        self._buffer[query] = self._buffer.get(query, 0) + self._increment

        # 3. trending reflects activity immediately, before the count is flushed.
        self._recency.record(query)

        self.searches_received += 1
        self._metrics.record_search()

        # 4. size-triggered flush. Bounded to one in-flight task (the lock
        #    serialises flushes anyway; this just avoids queueing a task per
        #    submission during a burst).
        if len(self._buffer) >= self._max_size and not self._pending_tasks:
            t = asyncio.create_task(self._safe_flush())
            self._pending_tasks.add(t)
            t.add_done_callback(self._pending_tasks.discard)

    async def _safe_flush(self) -> None:
        """Fire-and-forget flush wrapper that logs instead of dropping errors."""
        try:
            await self.flush()
        except Exception:
            log.exception("size-triggered flush failed; will retry on next interval")

    # -- flush --------------------------------------------------------------
    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._flush_interval)
            except asyncio.TimeoutError:
                pass  # normal: interval elapsed, time to flush
            try:
                await self.flush()
            except Exception:
                # A transient flush failure (e.g. DB locked) must NOT kill the
                # loop — the data is safe in the WAL segments and the next tick
                # retries. Without this guard one error stops all future flushes.
                log.exception("batch flush failed; will retry next interval")

    async def flush(self) -> int:
        """Flush outstanding work to the DB in one transaction. Returns the number
        of distinct queries written. Serialised by a lock; a no-op when there is
        nothing to write."""
        async with self._lock:
            if self._wal_enabled:
                return await self._flush_wal()
            return await self._flush_no_wal()

    async def _flush_wal(self) -> int:
        # 1. Rotate the current window (if any) into an immutable segment.
        if self._buffer:
            self._buffer = {}
            self._seg_seq += 1
            self._rotate_live_to_segment(self._seg_seq)

        # 2. Collect ALL outstanding segments (this window + any left behind by a
        #    previously-failed flush) and aggregate their lines.
        segments = self._list_segments()
        if not segments:
            return 0
        deltas: dict[str, int] = {}
        for _seq, path in segments:
            self._read_into(path, deltas)
        max_seq = segments[-1][0]

        # 3. Apply in one transaction, persisting the watermark atomically. On
        #    failure the segments stay on disk (buffer already cleared); the next
        #    flush retries them. No re-buffering => no duplication.
        new_counts = await asyncio.to_thread(self._storage.apply_batch, deltas, self._now(), max_seq)
        self._post_apply(new_counts, deltas)

        # 4. Durably committed: delete the applied segments.
        self._flush_seq = max_seq
        for _seq, path in segments:
            if os.path.exists(path):
                os.remove(path)
        return len(deltas)

    async def _flush_no_wal(self) -> int:
        """Flush path when the WAL is disabled: apply straight from the in-memory
        buffer, re-buffering on failure (no on-disk durability in this mode)."""
        if not self._buffer:
            return 0
        deltas = self._buffer
        self._buffer = {}
        try:
            new_counts = await asyncio.to_thread(self._storage.apply_batch, deltas, self._now(), None)
        except Exception:
            for q, d in deltas.items():
                self._buffer[q] = self._buffer.get(q, 0) + d
            raise
        self._post_apply(new_counts, deltas)
        return len(deltas)

    def _post_apply(self, new_counts: dict[str, int], deltas) -> None:
        """Reconcile the in-memory index + cache after a successful DB apply."""
        for q, c in new_counts.items():
            self._trie.upsert(q, c)
        self._invalidate_prefixes(deltas.keys())
        self.flushes += 1
        self.rows_flushed += len(deltas)

    def _invalidate_prefixes(self, queries) -> None:
        """Invalidate every prefix of every changed query, both ranking modes.

        Guarantees suggestions reflect the new counts immediately after a flush.
        Invalidating a key that isn't cached is a cheap no-op, and TTL is the
        backstop for anything we don't touch.
        """
        seen: set[str] = set()
        for q in queries:
            for i in range(1, len(q) + 1):
                p = q[:i]
                if p in seen:
                    continue
                seen.add(p)
                self._cache.invalidate_all_modes(p)

    # -- WAL helpers --------------------------------------------------------
    def _segment_path(self, seq: int) -> str:
        return f"{self._seg_prefix}{seq}"

    def _list_segments(self) -> list[tuple[int, str]]:
        """All segment files on disk, sorted by sequence number ascending."""
        out: list[tuple[int, str]] = []
        for path in glob.glob(self._seg_prefix + "*"):
            try:
                seq = int(path[len(self._seg_prefix):])
            except ValueError:
                continue
            out.append((seq, path))
        out.sort()
        return out

    def _rotate_live_to_segment(self, seq: int) -> None:
        """Close the live WAL, move it to an immutable segment, open a fresh one."""
        if not self._wal_enabled or self._wal_file is None:
            return
        self._wal_file.close()
        if os.path.exists(self._wal_path):
            os.replace(self._wal_path, self._segment_path(seq))
        self._wal_file = open(self._wal_path, "a", encoding="utf-8")

    def _read_into(self, path: str, deltas: dict[str, int]) -> int:
        """Aggregate the lines of a WAL file into ``deltas``. Returns line count."""
        n = 0
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                q = line.rstrip("\n")
                if not q:
                    continue
                deltas[q] = deltas.get(q, 0) + self._increment
                n += 1
        return n

    # -- stats --------------------------------------------------------------
    def stats(self) -> dict:
        write_reduction = (
            self.searches_received / self.rows_flushed if self.rows_flushed else 0.0
        )
        return {
            "searches_received": self.searches_received,
            "db_upserts": self.rows_flushed,
            "flushes": self.flushes,
            "buffer_size": len(self._buffer),
            "recovered_entries": self.recovered_entries,
            "last_flush_seq": self._flush_seq,
            # e.g. 12.5 => 1 DB write per 12.5 searches on average.
            "write_reduction_ratio": round(write_reduction, 3),
        }
