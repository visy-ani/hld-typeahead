"""Build the single-PDF Project Report required by the submission form.

The form asks for one PDF (<=10 MB) containing:
  1. Architecture diagram / clear architecture explanation
  2. Dataset source and loading instructions
  3. API documentation
  4. Explanations of design choices and trade-offs
  5. Performance report

This script assembles those from the repo's markdown docs (README + DESIGN +
PERFORMANCE), inlines the screenshots as base64, and renders a clean PDF with
headless Chromium (Playwright) — no LaTeX/pandoc needed.

Usage:
    pip install markdown playwright && playwright install chromium
    python -m scripts.build_report            # -> Project_Report.pdf
"""

from __future__ import annotations

import base64
import datetime
import re
import subprocess
import sys
from pathlib import Path

import markdown

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "Project_Report.pdf"

MD_EXT = ["extra", "tables", "fenced_code", "sane_lists", "toc"]


def commit_hash() -> str:
    try:
        return subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"]).decode().strip()
    except Exception:
        return "(uncommitted)"


def md_to_html(path: Path) -> str:
    text = path.read_text(encoding="utf-8")
    # Drop the leading H1 of each doc (we supply our own section headers).
    html = markdown.markdown(text, extensions=MD_EXT)
    return html


def inline_images(html: str) -> str:
    """Replace <img src="docs/screenshots/x.png"> with base64 data URIs."""
    def repl(m):
        src = m.group(1)
        p = (ROOT / src).resolve()
        if not p.exists():
            return m.group(0)
        data = base64.b64encode(p.read_bytes()).decode()
        ext = p.suffix.lstrip(".") or "png"
        return f'src="data:image/{ext};base64,{data}"'
    return re.sub(r'src="([^"]+)"', repl, html)


CSS = """
@page { size: A4; margin: 16mm 15mm; }
* { box-sizing: border-box; }
body { font-family: -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
       font-size: 11px; line-height: 1.5; color: #1a1a1a; }
h1 { font-size: 20px; border-bottom: 2px solid #2d6cdf; padding-bottom: 4px; margin-top: 0; }
h2 { font-size: 15px; color: #14315e; border-bottom: 1px solid #d8dee9; padding-bottom: 3px; margin-top: 22px; }
h3 { font-size: 12.5px; color: #2d3a4f; margin-top: 16px; }
h4 { font-size: 11.5px; color: #44506a; }
p, li { font-size: 11px; }
code { font-family: ui-monospace, "SF Mono", Menlo, Consolas, monospace; font-size: 9.5px;
       background: #f0f2f6; padding: 1px 4px; border-radius: 3px; }
pre { background: #f6f8fa; border: 1px solid #e1e6ef; border-radius: 6px; padding: 10px 12px;
      white-space: pre-wrap; word-break: break-word; overflow: hidden; }
pre code { background: none; padding: 0; font-size: 8.6px; line-height: 1.35; }
table { border-collapse: collapse; width: 100%; margin: 10px 0; font-size: 10px; }
th, td { border: 1px solid #cfd6e2; padding: 5px 8px; text-align: left; vertical-align: top; }
th { background: #eef2f8; }
img { max-width: 100%; height: auto; border: 1px solid #d8dee9; border-radius: 4px; }
a { color: #2d6cdf; text-decoration: none; }
blockquote { border-left: 3px solid #2d6cdf; margin: 8px 0; padding: 2px 12px; color: #44506a; background: #f7f9fc; }
hr { border: 0; border-top: 1px solid #d8dee9; margin: 16px 0; }
.section { page-break-before: always; }
.cover { text-align: center; padding-top: 120px; page-break-after: always; }
.cover h1 { font-size: 30px; border: 0; color: #14315e; }
.cover .sub { font-size: 14px; color: #5a6472; margin-top: 6px; }
.cover .meta { margin-top: 60px; font-size: 12px; line-height: 2; }
.cover .meta b { color: #14315e; }
.cover code { font-size: 10px; }
.contents { border: 1px solid #cfd6e2; border-radius: 8px; padding: 12px 18px; background: #f7f9fc; margin: 18px 0; }
.contents h2 { margin-top: 0; border: 0; }
"""


def build_html(student: str, repo: str, chash: str) -> str:
    date = datetime.date.today().isoformat()
    readme = inline_images(md_to_html(ROOT / "README.md"))
    design = md_to_html(ROOT / "DESIGN.md")
    perf = md_to_html(ROOT / "PERFORMANCE.md")

    cover = f"""
    <div class="cover">
      <h1>Search Typeahead System</h1>
      <div class="sub">Project Report</div>
      <div class="meta">
        <div><b>Student:</b> {student}</div>
        <div><b>Repository:</b> <a href="{repo}">{repo}</a></div>
        <div><b>Date:</b> {date}</div>
      </div>
    </div>
    """

    contents = """
    <div class="contents">
      <h2>Report contents (required sections)</h2>
      <ol>
        <li><b>Architecture diagram / explanation</b> — Part A &sect; Architecture (with read/write-path diagram)</li>
        <li><b>Dataset source &amp; loading instructions</b> — Part A &sect; Dataset</li>
        <li><b>API documentation</b> — Part A &sect; API</li>
        <li><b>Design choices &amp; trade-offs</b> — Part B (full design rationale)</li>
        <li><b>Performance report</b> — Part C (latency p95, cache hit rate, write reduction)</li>
      </ol>
    </div>
    """

    body = f"""
    <div class="section"><h1>Part A — Overview, Architecture, Dataset &amp; API</h1>{readme}</div>
    <div class="section"><h1>Part B — Design &amp; Rationale (choices &amp; trade-offs)</h1>{design}</div>
    <div class="section"><h1>Part C — Performance Report</h1>{perf}</div>
    """

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><style>{CSS}</style></head>
    <body>{cover}{contents}{body}</body></html>"""


def main() -> int:
    student = sys.argv[1] if len(sys.argv) > 1 else "Anish Yadav"
    repo = sys.argv[2] if len(sys.argv) > 2 else "https://github.com/visy-ani/hld-typeahead"
    chash = commit_hash()
    html = build_html(student, repo, chash)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("Playwright not installed. Run: pip install playwright && playwright install chromium", file=sys.stderr)
        return 1

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.set_content(html, wait_until="networkidle")
        page.pdf(path=str(OUT), format="A4", print_background=True,
                 margin={"top": "16mm", "bottom": "16mm", "left": "15mm", "right": "15mm"})
        browser.close()

    size_mb = OUT.stat().st_size / 1024 / 1024
    print(f"Wrote {OUT.name} ({size_mb:.2f} MB)  [commit {chash[:12]}...]")
    if size_mb > 10:
        print("WARNING: exceeds the 10 MB form limit!", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
