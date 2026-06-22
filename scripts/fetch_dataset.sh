#!/usr/bin/env bash
# Download the real word-frequency dataset (Peter Norvig's n-gram lists).
# These give 333k single keywords + 286k two-word phrases, each with a real
# corpus count. If you skip this, `load_dataset.py --source synthetic` works
# fully offline.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p data

echo "Downloading count_1w.txt (333k unigrams, ~5MB)..."
curl -L --fail -o data/count_1w.txt https://norvig.com/ngrams/count_1w.txt

echo "Downloading count_2w.txt (286k bigrams, ~5.5MB)..."
curl -L --fail -o data/count_2w.txt https://norvig.com/ngrams/count_2w.txt

echo "Done:"
wc -l data/count_1w.txt data/count_2w.txt
