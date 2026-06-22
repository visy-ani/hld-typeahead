"""Central configuration.

Every tunable lives here and can be overridden with an environment variable, so
the same code runs identically in tests, the demo, and the benchmark harness
without editing source. Defaults are chosen to be sensible for a local demo.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# Project paths -------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ[name])
    except (KeyError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ[name])
    except (KeyError, ValueError):
        return default


def _env_str(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    # --- Primary store -----------------------------------------------------
    db_path: str = field(default_factory=lambda: _env_str("TYPEAHEAD_DB", str(DATA_DIR / "typeahead.db")))

    # --- Suggestion index --------------------------------------------------
    # Suggestions returned to the client.
    suggest_limit: int = field(default_factory=lambda: _env_int("TYPEAHEAD_SUGGEST_LIMIT", 10))
    # Candidate pool kept per trie node. We keep more than `suggest_limit` so the
    # recency re-ranker has room to promote a trending query that is not in the
    # all-time top 10 for that prefix. Bigger pool = better recency recall, more memory.
    trie_node_capacity: int = field(default_factory=lambda: _env_int("TYPEAHEAD_TRIE_K", 25))

    # --- Distributed cache -------------------------------------------------
    cache_nodes: int = field(default_factory=lambda: _env_int("TYPEAHEAD_CACHE_NODES", 4))
    # Virtual nodes (replicas) per physical node on the hash ring. More replicas
    # => smoother key distribution and less data movement when a node is added/removed.
    cache_vnodes: int = field(default_factory=lambda: _env_int("TYPEAHEAD_CACHE_VNODES", 150))
    cache_ttl_seconds: float = field(default_factory=lambda: _env_float("TYPEAHEAD_CACHE_TTL", 30.0))
    # Max entries per cache node before LRU eviction kicks in.
    cache_capacity_per_node: int = field(default_factory=lambda: _env_int("TYPEAHEAD_CACHE_CAP", 5000))

    # --- Batch writer ------------------------------------------------------
    # Flush when EITHER the buffer reaches this many distinct queries...
    batch_max_size: int = field(default_factory=lambda: _env_int("TYPEAHEAD_BATCH_SIZE", 500))
    # ...OR this many seconds have elapsed since the last flush.
    batch_flush_interval: float = field(default_factory=lambda: _env_float("TYPEAHEAD_FLUSH_INTERVAL", 2.0))
    # Initial count assigned to a brand-new query the first time it is searched.
    new_query_initial_count: int = field(default_factory=lambda: _env_int("TYPEAHEAD_NEW_QUERY_COUNT", 1))
    # Append-only write-ahead log for crash recovery of un-flushed submissions.
    wal_enabled: bool = field(default_factory=lambda: _env_str("TYPEAHEAD_WAL", "1") not in ("0", "false", "False"))
    wal_path: str = field(default_factory=lambda: _env_str("TYPEAHEAD_WAL_PATH", str(DATA_DIR / "buffer.wal")))

    # --- Ranking (trending / recency) -------------------------------------
    # Default ranking mode for /suggest when the client doesn't specify one.
    default_ranking_mode: str = field(default_factory=lambda: _env_str("TYPEAHEAD_RANK_MODE", "basic"))
    # Half-life (seconds) of the recency score. After H seconds, a search's
    # contribution to the trending score halves. 1800s = 30 min by default.
    recency_half_life: float = field(default_factory=lambda: _env_float("TYPEAHEAD_HALF_LIFE", 1800.0))
    # Weight applied to the (normalised) recency score when blending with the
    # (log-compressed, normalised) historical score in 'trending' mode.
    recency_weight: float = field(default_factory=lambda: _env_float("TYPEAHEAD_RECENCY_WEIGHT", 0.6))
    history_weight: float = field(default_factory=lambda: _env_float("TYPEAHEAD_HISTORY_WEIGHT", 0.4))
    # Max number of distinct queries the recency tracker keeps live.
    trending_capacity: int = field(default_factory=lambda: _env_int("TYPEAHEAD_TRENDING_CAP", 50000))

    # --- Metrics -----------------------------------------------------------
    # Number of recent latency samples retained for percentile computation.
    latency_window: int = field(default_factory=lambda: _env_int("TYPEAHEAD_LATENCY_WINDOW", 20000))


settings = Settings()
