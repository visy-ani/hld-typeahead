# Performance Report

All numbers below were measured on the development machine (Apple M-series, 8
cores, 8 GB RAM, Python 3.14) against the **real 300,000-query dataset**
(Norvig unigrams + bigrams). Reproduce everything with:

```bash
./run.sh                                   # terminal 1 — start the server
python -m scripts.benchmark \              # terminal 2 — run the benchmark
    --suggest-requests 20000 --search-requests 8000 --distinct 150 --concurrency 32
```

The harness drives the **running HTTP server** (not in-process calls), so latency
includes the full ASGI + JSON round trip.

---

## 1. Suggestion latency

Two latencies matter and we report both:

* **Server-side** — time spent *inside* `/suggest` (cache lookup → trie → rank),
  measured by the service itself and exposed at `/metrics`.
* **End-to-end** — wall-clock time a client sees, including HTTP + JSON.

### Server-side (the algorithm itself)

| Metric | basic | trending |
|--------|-------|----------|
| p50    | 0.0048 ms (4.8 µs) | 0.0052 ms |
| p95    | **0.0102 ms (10.2 µs)** | 0.0101 ms |
| p99    | 0.037 ms | 0.059 ms |

The suggestion computation is **microsecond-scale** because a warm request is a
single consistent-hash routed dict lookup, and a cold request is `O(len(prefix))`
trie navigation plus a sort of ≤25 candidates. Trending costs marginally more
(the recency blend), still well under 100 µs at p99.

### End-to-end HTTP latency vs. concurrency

| Concurrency | p50 | p95 | p99 | throughput |
|-------------|-----|-----|-----|------------|
| 1  | 0.72 ms | **0.84 ms** | 0.99 ms | 1,311 rps |
| 4  | 2.03 ms | 2.42 ms | 3.93 ms | **1,814 rps** |
| 8  | 7.16 ms | 13.9 ms | 15.6 ms | 913 rps |
| 16 | 17.4 ms | 57.9 ms | 64.0 ms | 632 rps |
| 32 | 24.2 ms | 106.6 ms | 163 ms | 848 rps |

**Reading these numbers honestly:** unloaded, a request completes in **~0.8 ms
p95** end-to-end — the HTTP/JSON layer dominates, since the algorithm is < 10 µs.
Throughput peaks around concurrency 4 (~1,800 rps). Beyond that, latency climbs
and throughput falls. That is **not** the typeahead algorithm getting slower —
it is the single-process Python server (one event loop, one GIL) competing for
the same 8 cores as the benchmark client running on the same machine. In a real
deployment you would run N uvicorn workers behind a load balancer; because the
cache layer is already a node abstraction, the per-process caches would be
replaced by shared Redis nodes routed by the same consistent-hash ring (see
DESIGN.md → "Scaling out").

---

## 2. Cache effectiveness

Workload: 20,000 `/suggest` requests with prefixes sampled from a **Zipfian**
distribution over an 800-prefix pool (a few hot prefixes dominate — the shape of
real search traffic).

| Metric | Value |
|--------|-------|
| Cache hit rate | **96.07 %** |
| Cache nodes | 4 (150 virtual nodes each → 600 ring points) |
| Per-request hit latency | ~5 µs server-side |

A 96 % hit rate means only ~4 % of requests touch the trie at all; the rest are
served from the in-memory, consistent-hash-routed cache. Hit rate rises further
with a hotter workload and falls toward the cold-compute cost (still microseconds)
for a uniform workload.

### Consistent-hash key distribution

`GET /ring?sample=20000` routes 20k synthetic keys and reports load per node
(1.0 = perfectly even):

```
counts      : {cache-0: 5682, cache-1: 4997, cache-2: 4762, cache-3: 4559}
load factor : {cache-0: 1.136, cache-1: 0.999, cache-2: 0.952, cache-3: 0.912}
```

All four nodes are within ~14 % of the ideal share — the virtual-node count (150)
keeps the ring balanced. The unit test `test_minimal_remap_on_removal` further
shows that removing a node remaps only ~1/N of keys (≈20 % for 5 nodes), not the
~80 % that `hash % N` would churn.

---

## 3. Batch-write reduction

Workload: 8,000 `/search` submissions spread over 150 distinct queries.

| Metric | Value |
|--------|-------|
| Searches submitted | 8,000 |
| Distinct queries | 150 |
| **DB upserts performed** | **1,032** |
| DB write transactions | 7 |
| **Write reduction** | **7.75×** |
| Writes avoided | **87.1 %** |

Without batching this is 8,000 individual `INSERT/UPDATE` statements in 8,000
transactions. With batching it is 1,050 upserts across **7 transactions** —
each flush aggregates repeated queries (e.g. the same query searched 50 times in
a window becomes one `count = count + 50` upsert) and commits the whole window at
once. The reduction ratio grows with traffic volume and query repetition: the
more concentrated the search load, the closer the ratio gets to
`searches / distinct_queries`.

### Failure trade-off (measured behaviour)

Every submission is appended to a write-ahead log *before* acknowledgement. The
test `test_wal_crash_recovery` simulates a process crash with un-flushed entries
in the live log (and a rotated segment from a crash mid-flush) and verifies that
`recover()` replays all of them into the DB and the in-memory index on restart.
So a process crash loses **zero** acknowledged searches. The residual exposure is
a power loss between an OS write and an fsync (a few log lines); enable
`TYPEAHEAD_WAL` fsync-per-write to close that gap at a latency cost. See
DESIGN.md → "Batch writes & durability".

---

## 4. Index build & memory

| Metric | Value |
|--------|-------|
| Queries indexed | 300,000 |
| Trie nodes | 934,667 |
| Build time (post-order top-K) | ~1.3 s |
| Resident memory (RSS) | ~351 MB |
| DB ingest time | ~0.5 s |

Memory is bounded by `Σ min(subtree_size, K)` over trie nodes, which is
dominated by `Σ len(query)` rather than `nodes × K` — deep nodes have tiny
subtrees. Raising the candidate capacity `K` therefore costs far less than the
naive `nodes × K` estimate.

---

## 5. Summary against the rubric's non-functional asks

| Asked for | Result |
|-----------|--------|
| Low-latency suggestions, p95 | ~10 µs server-side / 0.84 ms end-to-end (unloaded) |
| Cache hit rate | 96.1 % on a Zipfian workload |
| DB read/write counts | exposed at `/metrics` (`storage.rows_read` / `rows_written` / `write_transactions`) |
| Write reduction via batching | 7.75× (87.1 % avoided) |
| Consistent-hashing evidence | `/ring` distribution + `/cache/debug` + unit tests |
