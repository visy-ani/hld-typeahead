"""Performance benchmark for the typeahead system.

Drives the *running* HTTP server (so the numbers include real network +
serialization cost, not just in-process calls) and reports:

  * /suggest latency: p50 / p95 / p99 / mean / max, and throughput
  * cache hit rate (from /metrics deltas over the suggest phase)
  * batch write-reduction: searches submitted vs DB upserts performed
  * DB read/write counts

Workload realism: prefixes are sampled from a Zipfian distribution over a fixed
pool, so a few hot prefixes repeat often — exactly the access pattern a real
search box produces, and the pattern that makes the cache earn its keep.

Usage:
    # start the server first:  uvicorn app.main:app --port 8077
    python -m scripts.benchmark --suggest-requests 20000 --search-requests 5000 --concurrency 32
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import statistics
import string
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx  # noqa: E402

from app.config import DATA_DIR  # noqa: E402


def percentile(sorted_vals, pct):
    if not sorted_vals:
        return 0.0
    k = pct / 100 * (len(sorted_vals) - 1)
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


def build_prefix_pool(size: int) -> list[str]:
    """A pool of plausible prefixes (1–4 chars). Real words bias toward common
    letter combinations, which is fine — we only need a stable, repeatable set."""
    rng = random.Random(123)
    letters = string.ascii_lowercase
    pool = set()
    # common short prefixes first (these will be the hot ones)
    for a in letters:
        pool.add(a)
    while len(pool) < size:
        n = rng.randint(2, 4)
        pool.add("".join(rng.choice(letters) for _ in range(n)))
    return sorted(pool)


def zipf_sample(pool: list[str], n: int, s: float = 1.1) -> list[str]:
    """Sample n prefixes Zipf-distributed over the pool (hot prefixes repeat)."""
    rng = random.Random(7)
    ranks = range(1, len(pool) + 1)
    weights = [1.0 / (r ** s) for r in ranks]
    return rng.choices(pool, weights=weights, k=n)


async def run_suggest_phase(base, requests, concurrency, mode):
    pool = build_prefix_pool(800)
    workload = zipf_sample(pool, requests)
    latencies: list[float] = []
    sem = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient(base_url=base, timeout=30) as client:
        # snapshot cache stats before
        m0 = (await client.get("/metrics")).json()

        async def one(prefix):
            async with sem:
                t = time.perf_counter()
                r = await client.get("/suggest", params={"q": prefix, "mode": mode})
                dt = (time.perf_counter() - t) * 1000
                r.raise_for_status()
                latencies.append(dt)

        t0 = time.perf_counter()
        await asyncio.gather(*(one(p) for p in workload))
        wall = time.perf_counter() - t0

        m1 = (await client.get("/metrics")).json()

    latencies.sort()
    hits = m1["cache"]["total_hits"] - m0["cache"]["total_hits"]
    lookups = m1["cache"]["lookups"] - m0["cache"]["lookups"]
    return {
        "mode": mode,
        "requests": len(latencies),
        "concurrency": concurrency,
        "wall_s": round(wall, 3),
        "throughput_rps": round(len(latencies) / wall, 1),
        "client_latency_ms": {
            "p50": round(percentile(latencies, 50), 4),
            "p95": round(percentile(latencies, 95), 4),
            "p99": round(percentile(latencies, 99), 4),
            "mean": round(statistics.fmean(latencies), 4),
            "max": round(latencies[-1], 4),
        },
        "server_latency_ms": m1["requests"]["latency"],
        "cache_hit_rate_this_phase": round(hits / lookups, 4) if lookups else None,
    }


async def run_search_phase(base, requests, concurrency, distinct):
    """Submit many searches over a small distinct set so aggregation is visible."""
    rng = random.Random(99)
    terms = [f"benchmark query {i}" for i in range(distinct)]
    workload = [rng.choice(terms) for _ in range(requests)]
    sem = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient(base_url=base, timeout=30) as client:
        m0 = (await client.get("/metrics")).json()

        async def one(q):
            async with sem:
                r = await client.post("/search", json={"query": q})
                r.raise_for_status()

        t0 = time.perf_counter()
        await asyncio.gather(*(one(q) for q in workload))
        wall = time.perf_counter() - t0

        # Wait for the batch writer to drain the buffer so the reduction is final.
        prev = -1
        for _ in range(60):
            m1 = (await client.get("/metrics")).json()
            buffered = m1["batch_writer"]["buffer_size"]
            upserts = m1["batch_writer"]["db_upserts"]
            if buffered == 0 and upserts == prev:
                break
            prev = upserts
            await asyncio.sleep(0.5)

    bw0, bw1 = m0["batch_writer"], m1["batch_writer"]
    searches = bw1["searches_received"] - bw0["searches_received"]
    upserts = bw1["db_upserts"] - bw0["db_upserts"]
    txns = m1["storage"]["write_transactions"] - m0["storage"]["write_transactions"]
    return {
        "searches_submitted": searches,
        "distinct_queries": distinct,
        "submit_wall_s": round(wall, 3),
        "submit_throughput_rps": round(requests / wall, 1),
        "db_upserts": upserts,
        "db_write_transactions": txns,
        "write_reduction_ratio": round(searches / upserts, 2) if upserts else None,
        "writes_avoided_pct": round((1 - upserts / searches) * 100, 2) if searches else None,
    }


async def main_async(args):
    base = args.base_url
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        try:
            h = (await client.get("/health")).json()
        except Exception as e:
            print(f"ERROR: cannot reach server at {base} ({e}).\n"
                  f"Start it with: uvicorn app.main:app --port 8077", file=sys.stderr)
            return 1
        print(f"Server OK: {h['indexed_queries']:,} indexed queries at {base}\n")

    results = {"base_url": base}

    print(f"[1/3] suggest latency (basic)  — {args.suggest_requests:,} reqs, c={args.concurrency}")
    results["suggest_basic"] = await run_suggest_phase(base, args.suggest_requests, args.concurrency, "basic")
    print(f"[2/3] suggest latency (trending) — {args.suggest_requests:,} reqs, c={args.concurrency}")
    results["suggest_trending"] = await run_suggest_phase(base, args.suggest_requests, args.concurrency, "trending")
    print(f"[3/3] batch write-reduction   — {args.search_requests:,} searches over {args.distinct} distinct")
    results["batch_writes"] = await run_search_phase(base, args.search_requests, args.concurrency, args.distinct)

    _print_report(results)
    out = DATA_DIR / "benchmark_results.json"
    out.write_text(json.dumps(results, indent=2))
    print(f"\nFull results written to {out}")
    return 0


def _print_report(r):
    print("\n" + "=" * 64)
    print("PERFORMANCE REPORT")
    print("=" * 64)
    for key in ("suggest_basic", "suggest_trending"):
        s = r[key]
        cl = s["client_latency_ms"]
        print(f"\n/suggest [{s['mode']}]  ({s['requests']:,} reqs @ c={s['concurrency']})")
        print(f"  throughput      : {s['throughput_rps']:,} req/s")
        print(f"  client latency  : p50={cl['p50']}ms  p95={cl['p95']}ms  p99={cl['p99']}ms  max={cl['max']}ms")
        sv = s["server_latency_ms"]
        print(f"  server latency  : p50={sv.get('p50_ms')}ms  p95={sv.get('p95_ms')}ms  p99={sv.get('p99_ms')}ms")
        print(f"  cache hit rate  : {s['cache_hit_rate_this_phase']}")
    b = r["batch_writes"]
    print(f"\nbatch writes")
    print(f"  searches submitted : {b['searches_submitted']:,} over {b['distinct_queries']:,} distinct queries")
    print(f"  DB upserts         : {b['db_upserts']:,} in {b['db_write_transactions']:,} transactions")
    print(f"  write reduction    : {b['write_reduction_ratio']}x  ({b['writes_avoided_pct']}% of writes avoided)")
    print("=" * 64)


def main():
    ap = argparse.ArgumentParser(description="Benchmark the typeahead server.")
    ap.add_argument("--base-url", default="http://127.0.0.1:8077")
    ap.add_argument("--suggest-requests", type=int, default=20000)
    ap.add_argument("--search-requests", type=int, default=5000)
    ap.add_argument("--distinct", type=int, default=200, help="distinct queries in the search phase")
    ap.add_argument("--concurrency", type=int, default=32)
    args = ap.parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
