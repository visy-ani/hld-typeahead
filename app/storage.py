"""Primary data store — the durable source of truth for query counts.

SQLite is used because it is the simplest thing that is *actually durable*: a
single file, zero setup, ACID transactions, and it survives restarts — exactly
"reliable enough for the assignment demo" while still being a real database.

Responsibilities
----------------
* Hold the authoritative ``query -> count`` table.
* Stream all rows at startup so the in-memory trie can be (re)built.
* Apply **batched** count increments from the batch writer in a single
  transaction (the whole point of batching — one commit for many searches).

Concurrency: the connection is opened ``check_same_thread=False`` and guarded by
a lock, so the async batch writer can run ``apply_batch`` inside a worker thread
(``asyncio.to_thread``) without ever blocking the event loop that serves
/suggest. That keeps suggestion latency flat even while a flush is happening.
"""

from __future__ import annotations

import sqlite3
import threading
from typing import Iterable, Iterator


SCHEMA = """
CREATE TABLE IF NOT EXISTS queries (
    query         TEXT PRIMARY KEY,
    count         INTEGER NOT NULL,
    last_searched REAL
);
CREATE INDEX IF NOT EXISTS idx_queries_count ON queries(count DESC);

-- Small key/value table for durable metadata. Currently holds the batch
-- writer's high-water mark (last successfully-applied flush sequence number),
-- which makes WAL replay exactly-once across crashes.
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class Storage:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        # WAL journal => readers don't block the writer and vice-versa.
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.executescript(SCHEMA)
        self._conn.commit()
        # Observability counters (the rubric asks for DB read/write counts).
        self.rows_read = 0
        self.rows_written = 0
        self.write_transactions = 0

    # -- ingestion (used by the loader script) ------------------------------
    def bulk_replace(self, items: Iterable[tuple[str, int]], batch: int = 10000) -> int:
        """Replace the table contents with ``items``. Returns rows written."""
        total = 0
        with self._lock:
            self._conn.execute("DELETE FROM queries;")
            buf: list[tuple[str, int, float | None]] = []
            for query, count in items:
                buf.append((query, count, None))
                if len(buf) >= batch:
                    self._conn.executemany(
                        "INSERT OR REPLACE INTO queries(query, count, last_searched) VALUES (?,?,?)", buf
                    )
                    total += len(buf)
                    buf.clear()
            if buf:
                self._conn.executemany(
                    "INSERT OR REPLACE INTO queries(query, count, last_searched) VALUES (?,?,?)", buf
                )
                total += len(buf)
            self._conn.commit()
        return total

    # -- startup: rebuild the index ----------------------------------------
    def load_all(self) -> Iterator[tuple[str, int]]:
        """Stream every (query, count). Used to build the trie at startup."""
        with self._lock:
            cur = self._conn.execute("SELECT query, count FROM queries")
            while True:
                rows = cur.fetchmany(10000)
                if not rows:
                    break
                self.rows_read += len(rows)
                for query, count in rows:
                    yield query, count

    def count_rows(self) -> int:
        with self._lock:
            return self._conn.execute("SELECT COUNT(*) FROM queries").fetchone()[0]

    # -- batched writes (the hot write path, called by the batch writer) ----
    def apply_batch(self, deltas: dict[str, int], timestamp: float, seq: int | None = None) -> dict[str, int]:
        """Apply aggregated increments in ONE transaction.

        ``deltas`` maps query -> total increment accumulated since the last
        flush. Returns ``query -> new_count`` so the caller can refresh the trie
        and decide cache invalidation. This is the method that turns N search
        requests into 1 database transaction.

        If ``seq`` is given, the high-water mark ``meta['last_flush_seq']`` is
        updated **in the same transaction**, so the counts and the watermark
        commit atomically — the basis for exactly-once WAL recovery.
        """
        if not deltas:
            return {}
        items = list(deltas.items())
        with self._lock:
            # Upsert: create the row if missing, otherwise add the delta.
            self._conn.executemany(
                """
                INSERT INTO queries(query, count, last_searched)
                VALUES (?, ?, ?)
                ON CONFLICT(query) DO UPDATE SET
                    count = count + excluded.count,
                    last_searched = excluded.last_searched
                """,
                [(q, d, timestamp) for q, d in items],
            )
            if seq is not None:
                self._conn.execute(
                    "INSERT INTO meta(key, value) VALUES('last_flush_seq', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (str(seq),),
                )
            self._conn.commit()
            self.rows_written += len(items)
            self.write_transactions += 1
            # Read back the authoritative new counts in one query.
            placeholders = ",".join("?" for _ in items)
            cur = self._conn.execute(
                f"SELECT query, count FROM queries WHERE query IN ({placeholders})",
                [q for q, _ in items],
            )
            self.rows_read += len(items)
            return {q: c for q, c in cur.fetchall()}

    def get_count(self, query: str) -> int | None:
        with self._lock:
            row = self._conn.execute("SELECT count FROM queries WHERE query = ?", (query,)).fetchone()
            self.rows_read += 1
            return row[0] if row else None

    def get_meta_int(self, key: str, default: int = 0) -> int:
        with self._lock:
            row = self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
            if not row:
                return default
            try:
                return int(row[0])
            except (TypeError, ValueError):
                return default

    def set_meta(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, str(value)),
            )
            self._conn.commit()

    def stats(self) -> dict[str, int]:
        return {
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "write_transactions": self.write_transactions,
        }

    def close(self) -> None:
        with self._lock:
            self._conn.close()
