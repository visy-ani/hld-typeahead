"""Capture UI screenshots with Playwright (for the submission's demo section).

Pre-seeds a few realistic searches so the trending + metrics panels are
populated, then drives the running UI and saves PNGs to docs/screenshots/.

Usage:
    # server must be running:  uvicorn app.main:app --port 8077
    pip install playwright && playwright install chromium
    python -m scripts.screenshot
"""

from __future__ import annotations

import json
import sys
import time
import urllib.request
from pathlib import Path

BASE = "http://127.0.0.1:8077"
OUT = Path(__file__).resolve().parent.parent / "docs" / "screenshots"
OUT.mkdir(parents=True, exist_ok=True)

SEED = {
    "data mining": 70, "london hotels": 55, "machine learning": 60,
    "new york city": 45, "java tutorial": 40, "python pandas": 35,
    "data science": 30, "high school": 25,
}


def post_search(q: str) -> None:
    req = urllib.request.Request(
        f"{BASE}/search", data=json.dumps({"query": q}).encode(),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    urllib.request.urlopen(req, timeout=5).read()


def seed() -> None:
    print("seeding searches...")
    for q, n in SEED.items():
        for _ in range(n):
            post_search(q)
    # wait for the batch writer to flush + invalidate caches
    time.sleep(3)


def main() -> int:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("Playwright not installed. Run: pip install playwright && playwright install chromium", file=sys.stderr)
        return 1

    seed()
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 880}, device_scale_factor=2)
        page.goto(BASE, wait_until="networkidle")
        time.sleep(1.0)

        # 1) Hero / landing with trending + metrics populated
        page.screenshot(path=str(OUT / "01-overview.png"))
        print("saved 01-overview.png")

        # 2) Suggestions dropdown (basic mode)
        page.fill("#search-input", "")
        page.type("#search-input", "lond", delay=60)
        page.wait_for_selector("#suggestions li", timeout=3000)
        time.sleep(0.4)
        page.screenshot(path=str(OUT / "02-suggestions-basic.png"))
        print("saved 02-suggestions-basic.png")

        # 3) Trending mode reordering for a bursted prefix
        page.click('.mode-btn[data-mode="trending"]')
        page.fill("#search-input", "")
        page.type("#search-input", "data", delay=60)
        page.wait_for_selector("#suggestions li", timeout=3000)
        time.sleep(0.5)
        page.screenshot(path=str(OUT / "03-suggestions-trending.png"))
        print("saved 03-suggestions-trending.png")

        # 4) Search submitted -> dummy response card
        page.fill("#search-input", "")
        page.type("#search-input", "machine learning", delay=40)
        time.sleep(0.3)
        page.keyboard.press("Enter")
        page.wait_for_selector("#result-card:not([hidden])", timeout=3000)
        time.sleep(0.5)
        page.screenshot(path=str(OUT / "04-search-response.png"))
        print("saved 04-search-response.png")

        browser.close()
    print(f"\nScreenshots in {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
