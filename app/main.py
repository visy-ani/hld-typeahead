"""FastAPI application — HTTP surface for the typeahead system.

Endpoints (see README for full docs):
    GET  /suggest?q=&mode=&limit=   suggestions for a prefix
    POST /search                     submit a search ({"query": ...})
    GET  /cache/debug?prefix=&mode=  which cache node owns a prefix + hit/miss
    GET  /trending?n=                recency-ranked trending queries
    GET  /metrics                    latency p95, cache hit rate, DB counts, ...
    GET  /ring                       consistent-hash key-distribution evidence
    GET  /health                     liveness
    GET  /                           the web UI

Lifecycle: on startup we build the trie from the DB, replay the WAL (crash
recovery), then start the background batch-writer flush loop. On shutdown we stop
the loop, flush the buffer, and close the DB.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .config import settings
from .service import SuggestionService

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("typeahead")

STATIC_DIR = Path(__file__).resolve().parent / "static"


class SearchBody(BaseModel):
    query: str = Field(..., description="The search query to submit", examples=["iphone 15"])


@asynccontextmanager
async def lifespan(app: FastAPI):
    service = SuggestionService(settings)
    app.state.service = service

    rows = service.storage.count_rows()
    if rows == 0:
        log.warning(
            "Primary store is EMPTY. Run `python -m scripts.load_dataset` to ingest a dataset "
            "before suggestions will work."
        )
    log.info("Building suggestion index from %d rows...", rows)
    indexed = service.load_index()
    log.info("Indexed %d queries into %d trie nodes.", indexed, service.trie.node_count)

    recovered = service.recover()
    if recovered:
        log.info("Recovered %d un-flushed search(es) from WAL.", recovered)

    service.start_background()
    log.info(
        "Ready: %d cache nodes (%d vnodes each), flush every %.1fs or %d queries.",
        settings.cache_nodes, settings.cache_vnodes, settings.batch_flush_interval, settings.batch_max_size,
    )
    try:
        yield
    finally:
        log.info("Shutting down: flushing buffer...")
        await service.shutdown()
        log.info("Shutdown complete.")


app = FastAPI(
    title="Search Typeahead System",
    version="1.0.0",
    description="Low-latency prefix suggestions with a consistent-hashed distributed cache, "
                "recency-aware trending, and batched writes.",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)


def _service(app: FastAPI) -> SuggestionService:
    return app.state.service


# -- API --------------------------------------------------------------------
@app.get("/suggest")
async def suggest(
    q: str | None = Query(default="", description="Prefix the user has typed"),
    mode: str | None = Query(default=None, description="basic | trending"),
    limit: int = Query(default=settings.suggest_limit, ge=1, le=50),
):
    return _service(app).suggest(q, mode=mode, limit=limit)


@app.post("/search")
async def search(body: SearchBody):
    return _service(app).search(body.query)


@app.get("/cache/debug")
async def cache_debug(
    prefix: str = Query(default="", description="Prefix to route through the ring"),
    mode: str | None = Query(default=None, description="basic | trending"),
):
    return _service(app).cache_debug(prefix, mode=mode)


@app.get("/trending")
async def trending(n: int = Query(default=10, ge=1, le=100)):
    return {"trending": _service(app).trending(n)}


@app.get("/metrics")
async def metrics():
    return _service(app).metrics_summary()


@app.get("/ring")
async def ring(sample: int = Query(default=5000, ge=100, le=100000)):
    return _service(app).ring_distribution(sample)


@app.get("/health")
async def health():
    svc = _service(app)
    return {"status": "ok", "indexed_queries": len(svc.trie)}


# -- UI ---------------------------------------------------------------------
@app.get("/", include_in_schema=False)
async def index():
    idx = STATIC_DIR / "index.html"
    if not idx.exists():
        return JSONResponse({"error": "UI not found", "hint": "static/index.html missing"}, status_code=404)
    return FileResponse(idx)


if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
