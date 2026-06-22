#!/usr/bin/env bash
# One-command setup + run.
#   ./run.sh            -> set up venv, fetch+load dataset (if empty), start server
#   ./run.sh --synthetic-> use the offline synthetic dataset instead of downloading
#   PORT=9000 ./run.sh  -> run on a different port
set -euo pipefail
cd "$(dirname "$0")"

PORT="${PORT:-8077}"
PY=".venv/bin/python"

# 1. virtualenv + deps
if [ ! -d ".venv" ]; then
  echo "==> creating virtualenv"
  python3 -m venv .venv
fi
echo "==> installing dependencies"
$PY -m pip install -q --upgrade pip
$PY -m pip install -q -r requirements.txt

# 2. dataset (only if the DB doesn't exist yet)
if [ ! -f "data/typeahead.db" ]; then
  if [ "${1:-}" == "--synthetic" ]; then
    echo "==> loading synthetic dataset"
    $PY -m scripts.load_dataset --source synthetic --limit 300000
  else
    if [ ! -f "data/count_1w.txt" ]; then
      echo "==> fetching real dataset"
      bash scripts/fetch_dataset.sh || echo "   (download failed — will fall back to synthetic)"
    fi
    echo "==> loading dataset"
    $PY -m scripts.load_dataset --limit 300000
  fi
else
  echo "==> data/typeahead.db already exists, skipping load (delete it to re-ingest)"
fi

# 3. run
echo "==> starting server on http://127.0.0.1:${PORT}  (UI at /, docs at /docs)"
exec $PY -m uvicorn app.main:app --host 127.0.0.1 --port "$PORT"
