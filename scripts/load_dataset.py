"""Dataset ingestion.

Loads a corpus of (query, count) pairs into the SQLite primary store. Two sources:

  --source wordfreq   Real data: Peter Norvig's word-frequency lists
                      (count_1w.txt = 333k single keywords, count_2w.txt =
                      286k two-word phrases), each with an empirical corpus
                      count. Single + two-word entries give us both keywords and
                      realistic multi-word "search queries" (e.g. "high school").
                      Source: https://norvig.com/ngrams/

  --source synthetic  Offline fallback: deterministically generates 100k+ queries
                      with a Zipfian count distribution (a few head queries that
                      are searched enormously often, a long tail searched rarely)
                      — the same shape real search traffic has.

  --source auto       (default) wordfreq if the files exist, else synthetic.

The dataset requirement (>=100k queries, each with a count) is satisfied by both.

Examples
--------
    python -m scripts.load_dataset                       # auto, top 200k
    python -m scripts.load_dataset --source wordfreq --limit 300000
    python -m scripts.load_dataset --source synthetic --limit 150000 --seed 7
"""

from __future__ import annotations

import argparse
import math
import os
import random
import re
import sys
import time
from pathlib import Path
from typing import Iterator

# Allow `python -m scripts.load_dataset` from the project root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import DATA_DIR, settings  # noqa: E402
from app.storage import Storage  # noqa: E402

UNIGRAMS = DATA_DIR / "count_1w.txt"
BIGRAMS = DATA_DIR / "count_2w.txt"

# A query must be lowercase tokens of letters/digits, each containing >=1 letter,
# separated by single spaces. Filters out the corpus noise (e.g. "0uplink").
_TOKEN = re.compile(r"^[a-z0-9]*[a-z][a-z0-9]*$")


def _clean(text: str) -> str | None:
    text = text.strip().lower()
    if not text:
        return None
    tokens = text.split()
    if not tokens or len(tokens) > 5:
        return None
    for tok in tokens:
        if not _TOKEN.match(tok):
            return None
    return " ".join(tokens)


def _read_freq_file(path: Path) -> Iterator[tuple[str, int]]:
    with open(path, "r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) != 2:
                continue
            query, raw_count = parts
            cleaned = _clean(query)
            if cleaned is None:
                continue
            try:
                count = int(raw_count)
            except ValueError:
                continue
            if count <= 0:
                continue
            yield cleaned, count


def load_wordfreq(limit: int, include_bigrams: bool) -> list[tuple[str, int]]:
    if not UNIGRAMS.exists():
        raise FileNotFoundError(
            f"{UNIGRAMS} not found. Download it with:\n"
            f"  curl -L -o {UNIGRAMS} https://norvig.com/ngrams/count_1w.txt\n"
            f"  curl -L -o {BIGRAMS} https://norvig.com/ngrams/count_2w.txt\n"
            "or run with --source synthetic."
        )
    merged: dict[str, int] = {}
    sources = [UNIGRAMS]
    if include_bigrams and BIGRAMS.exists():
        sources.append(BIGRAMS)
    for src in sources:
        for query, count in _read_freq_file(src):
            # Keep the max count if the same normalised query appears twice.
            if count > merged.get(query, 0):
                merged[query] = count
    # Top-N by count.
    items = sorted(merged.items(), key=lambda kv: (-kv[1], kv[0]))
    return items[:limit]


# -- synthetic generator ----------------------------------------------------
_HEADS = (
    "iphone ipad macbook samsung galaxy pixel laptop headphones earbuds keyboard mouse monitor "
    "camera drone printer router speaker smartwatch charger cable adapter ssd gpu cpu motherboard "
    "python java javascript golang rust kotlin swift typescript react angular vue django flask "
    "spring node express kubernetes docker terraform ansible kafka redis postgres mysql mongodb "
    "machine learning deep learning data science neural network transformer llm gpt embedding "
    "pizza burger sushi pasta tacos ramen coffee tea smoothie salad sandwich biryani noodles "
    "shoes sneakers jacket jeans tshirt dress watch backpack sunglasses wallet perfume "
    "london paris tokyo york berlin sydney dubai mumbai delhi singapore toronto rome madrid "
    "movie series anime documentary podcast album song guitar piano drums violin "
    "car bike truck scooter tesla toyota honda ford bmw audi mercedes hyundai "
    "hotel flight ticket resort beach mountain trek safari cruise visa passport"
).split()

_MODIFIERS = (
    "review price near me online cheap best 2024 2025 vs pro max mini deals discount "
    "tutorial guide course free download install setup error fix update version "
    "for beginners advanced tips recipe ideas comparison specs features alternative"
).split()


def load_synthetic(limit: int, seed: int) -> list[tuple[str, int]]:
    """Generate `limit` plausible queries with a Zipfian count distribution.

    Queries are built in order of increasing specificity — single keywords, then
    two-word phrases, then three-word phrases — and counts are assigned by rank so
    that broad head terms (e.g. "iphone") get the largest counts and the long
    tail of specific phrases gets the smallest, exactly the head-and-tail shape of
    real search traffic. No filler/padding tokens: every query reads naturally.
    """
    heads = _HEADS                      # ~130 single keywords (iphone, python, ...)
    mods = _MODIFIERS                   # ~40 modifiers (review, price, tutorial, ...)
    out: list[str] = []
    seen: set[str] = set()

    def add(q: str) -> None:
        if q not in seen:
            seen.add(q)
            out.append(q)

    # 1-word, then 2-word (head+modifier, head+head), then 3-word (head head mod).
    # Together these yield >600k distinct natural queries — plenty for any limit.
    for h in heads:
        if len(out) >= limit:
            break
        add(h)
    for h in heads:
        for m in mods:
            if len(out) >= limit:
                break
            add(f"{h} {m}")
        if len(out) >= limit:
            break
    for h in heads:
        for h2 in heads:
            if h != h2 and len(out) < limit:
                add(f"{h} {h2}")
        if len(out) >= limit:
            break
    for h in heads:
        for h2 in heads:
            if h == h2:
                continue
            for m in mods:
                if len(out) >= limit:
                    break
                add(f"{h} {h2} {m}")
            if len(out) >= limit:
                break
        if len(out) >= limit:
            break

    if len(out) < limit:
        print(f"[warning] synthetic vocabulary exhausted at {len(out):,} queries "
              f"(< requested {limit:,}). Increase the vocabulary or lower --limit.", file=sys.stderr)

    # Zipfian counts by rank: rank-1 ~ C, rank-r ~ C / r^s. Deterministic per seed.
    rng = random.Random(seed)
    C = 10_000_000
    items: list[tuple[str, int]] = []
    for rank, q in enumerate(out, start=1):
        base = C / (rank ** 1.07)
        jitter = 0.9 + 0.2 * rng.random()      # mild ±10% so counts aren't perfectly smooth
        items.append((q, max(1, int(base * jitter))))
    items.sort(key=lambda kv: (-kv[1], kv[0]))
    return items


# -- driver -----------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="Load a search-query dataset into the primary store.")
    ap.add_argument("--source", choices=["auto", "wordfreq", "synthetic"], default="auto")
    ap.add_argument("--limit", type=int, default=300_000, help="Max queries to ingest (>=100k recommended)")
    ap.add_argument("--no-bigrams", action="store_true", help="wordfreq: ingest single keywords only")
    ap.add_argument("--seed", type=int, default=42, help="synthetic: RNG seed for reproducibility")
    ap.add_argument("--db", default=settings.db_path, help="SQLite DB path")
    args = ap.parse_args()

    source = args.source
    if source == "auto":
        source = "wordfreq" if UNIGRAMS.exists() else "synthetic"
        print(f"[auto] selected source: {source}")

    t0 = time.perf_counter()
    if source == "wordfreq":
        items = load_wordfreq(args.limit, include_bigrams=not args.no_bigrams)
    else:
        items = load_synthetic(args.limit, args.seed)
    load_s = time.perf_counter() - t0

    if len(items) < 100_000:
        print(f"[warning] only {len(items):,} queries — assignment expects >=100,000. "
              f"Increase --limit or add a larger dataset.", file=sys.stderr)

    # Remove any stale WAL (live log + sequence segments + legacy file) so the
    # fresh dataset isn't polluted by old buffered writes.
    import glob as _glob
    stale = [settings.wal_path, settings.wal_path + ".flushing"] + _glob.glob(settings.wal_path + ".seg.*")
    for wal in stale:
        if os.path.exists(wal):
            os.remove(wal)

    storage = Storage(args.db)
    t1 = time.perf_counter()
    written = storage.bulk_replace(items)
    storage.set_meta("last_flush_seq", "0")   # reset the batch-writer watermark
    write_s = time.perf_counter() - t1
    storage.close()

    multiword = sum(1 for q, _ in items if " " in q)
    print("\n=== Ingestion complete ===")
    print(f"source           : {source}")
    print(f"db               : {args.db}")
    print(f"queries ingested : {written:,}")
    print(f"  single-word    : {written - multiword:,}")
    print(f"  multi-word     : {multiword:,}")
    print(f"count range      : {items[-1][1]:,} .. {items[0][1]:,}")
    print(f"prepare time     : {load_s:.2f}s")
    print(f"db write time    : {write_s:.2f}s")
    print("\ntop 10 by count:")
    for q, c in items[:10]:
        print(f"  {c:>14,}  {q}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
