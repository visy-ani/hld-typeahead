"use strict";

// ---- DOM ----
const $ = (id) => document.getElementById(id);
const input = $("search-input");
const box = $("suggestions");
const searchBtn = $("search-btn");
const resultCard = $("result-card");
const resultQuery = $("result-query");
const resultJson = $("result-json");
const trendingList = $("trending-list");
const statusLine = $("status-line");

// ---- state ----
let mode = "basic";
let suggestions = [];
let activeIndex = -1;
let debounceTimer = null;
let reqSeq = 0;            // guards against out-of-order /suggest responses
let searchSeq = 0;        // guards against out-of-order /search responses
const DEBOUNCE_MS = 250;   // wait this long after the last keystroke before querying

// ---- helpers ----
function setStatus(msg, isError = false) {
  statusLine.textContent = msg;
  statusLine.classList.toggle("error", isError);
}

function fmtCount(n) {
  if (n >= 1e9) return (n / 1e9).toFixed(1) + "B";
  if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (n >= 1e3) return (n / 1e3).toFixed(1) + "K";
  return String(n);
}

function escapeHtml(s) {
  return s.replace(/[&<>"']/g, (c) => (
    { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]
  ));
}

function highlight(text, prefix) {
  const safe = escapeHtml(text);
  if (!prefix) return safe;
  const p = prefix.toLowerCase();
  if (text.toLowerCase().startsWith(p)) {
    return "<mark>" + escapeHtml(text.slice(0, prefix.length)) + "</mark>" + escapeHtml(text.slice(prefix.length));
  }
  return safe;
}

function openBox() { box.hidden = false; input.setAttribute("aria-expanded", "true"); }
function closeBox() {
  box.hidden = true;
  input.setAttribute("aria-expanded", "false");
  input.removeAttribute("aria-activedescendant");
  activeIndex = -1;
}

function syncActiveDescendant() {
  // Tell assistive tech which option is keyboard-active.
  if (activeIndex >= 0) input.setAttribute("aria-activedescendant", "opt-" + activeIndex);
  else input.removeAttribute("aria-activedescendant");
}

// ---- request strip (system internals) ----
function updateStrip(data) {
  const cacheChip = $("chip-cache");
  cacheChip.textContent = "cache " + (data.cache || "—").toUpperCase();
  cacheChip.className = "chip " + (data.cache === "hit" ? "hit" : data.cache === "miss" ? "miss" : "");
  $("chip-node").textContent = "node " + (data.node || "—");
  $("chip-latency").textContent = "latency " + (data.latency_ms != null ? data.latency_ms + " ms" : "—");
  $("chip-count").textContent = "results " + (data.count != null ? data.count : "—");
}

// ---- suggestions ----
function renderState(html, isError = false) {
  box.innerHTML = `<li class="state${isError ? " error" : ""}">${html}</li>`;
  openBox();
}

function renderSuggestions(prefix) {
  if (!suggestions.length) {
    renderState(`No matches for “${escapeHtml(prefix)}”.`);
    return;
  }
  const maxCount = Math.max(...suggestions.map((s) => s.count), 1);
  const logMax = Math.log(maxCount + 1);
  box.innerHTML = suggestions.map((s, i) => {
    const barW = Math.round((Math.log(s.count + 1) / logMax) * 100);
    const rec = (mode === "trending" && s.recency > 0) ? `<span class="s-rec" title="recency score">▲${s.recency.toFixed(1)}</span>` : "";
    return `<li role="option" id="opt-${i}" data-i="${i}" aria-selected="${i === activeIndex}" class="${i === activeIndex ? "active" : ""}">
      <span class="s-rank">${i + 1}</span>
      <span class="s-text">${highlight(s.query, prefix)}</span>
      <span class="s-meta">
        ${rec}
        <span class="s-bar"><i style="width:${barW}%"></i></span>
        <span class="s-count">${fmtCount(s.count)}</span>
      </span>
    </li>`;
  }).join("");
  openBox();
}

async function fetchSuggestions() {
  const q = input.value;
  if (!q.trim()) { suggestions = []; closeBox(); updateStrip({}); return; }
  const seq = ++reqSeq;
  renderState(`<span class="spinner"></span>Searching…`);
  try {
    const res = await fetch(`/suggest?q=${encodeURIComponent(q)}&mode=${mode}`);
    if (!res.ok) throw new Error("HTTP " + res.status);
    const data = await res.json();
    if (seq !== reqSeq) return;          // a newer keystroke already fired
    suggestions = data.suggestions || [];
    activeIndex = -1;
    updateStrip(data);
    renderSuggestions(data.prefix || q.trim());
    setStatus(`Suggest "${data.prefix}" · ${data.cache} · ${data.latency_ms} ms · node ${data.node}`);
  } catch (err) {
    if (seq !== reqSeq) return;
    suggestions = [];
    renderState(`Error fetching suggestions: ${escapeHtml(err.message)}`, true);
    setStatus("Suggestion request failed: " + err.message, true);
  }
}

function onInput() {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(fetchSuggestions, DEBOUNCE_MS);
}

// ---- keyboard navigation ----
function moveActive(delta) {
  if (!suggestions.length) return;
  activeIndex = (activeIndex + delta + suggestions.length) % suggestions.length;
  renderSuggestions(input.value.trim());
  syncActiveDescendant();
  const el = box.querySelector(`li[data-i="${activeIndex}"]`);
  if (el) el.scrollIntoView({ block: "nearest" });
}

function onKeyDown(e) {
  switch (e.key) {
    case "ArrowDown": e.preventDefault(); if (box.hidden) fetchSuggestions(); else moveActive(1); break;
    case "ArrowUp": e.preventDefault(); if (!box.hidden) moveActive(-1); break;
    case "Enter":
      e.preventDefault();
      if (activeIndex >= 0 && suggestions[activeIndex]) {
        submitSearch(suggestions[activeIndex].query);
      } else {
        submitSearch(input.value);
      }
      break;
    case "Escape": closeBox(); break;
  }
}

// ---- search submission ----
async function submitSearch(query) {
  query = (query || "").trim();
  if (!query) return;
  input.value = query;
  closeBox();
  const seq = ++searchSeq;
  setStatus(`Submitting search "${query}"…`);
  try {
    const res = await fetch("/search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query }),
    });
    if (!res.ok) throw new Error("HTTP " + res.status);
    const data = await res.json();
    if (seq !== searchSeq) return;     // a newer search already resolved
    resultQuery.textContent = data.query ? `"${data.query}"` : "";
    resultJson.textContent = JSON.stringify(data, null, 2);
    resultCard.hidden = false;
    setStatus(`Searched "${data.query}". Count buffered for batch write.`);
    // The search just changed counts + recency: refresh the live panels.
    loadTrending();
    loadMetrics();
  } catch (err) {
    setStatus("Search failed: " + err.message, true);
  }
}

// ---- trending ----
async function loadTrending() {
  try {
    const res = await fetch("/trending?n=10");
    const data = await res.json();
    const items = data.trending || [];
    if (!items.length) {
      trendingList.innerHTML = `<li class="empty">No searches yet — submit a few to see trends.</li>`;
      return;
    }
    trendingList.innerHTML = items.map((t) => `
      <li>
        <span class="t-rank"></span>
        <span class="t-query" data-q="${escapeHtml(t.query)}">${escapeHtml(t.query)}</span>
        <span class="t-score" title="recency score / all-time count">▲${t.recency_score.toFixed(1)} · ${fmtCount(t.count)}</span>
      </li>`).join("");
  } catch (err) {
    trendingList.innerHTML = `<li class="empty">Failed to load trending.</li>`;
  }
}

// ---- metrics ----
async function loadMetrics() {
  try {
    const res = await fetch("/metrics");
    const m = await res.json();
    $("m-p95").textContent = (m.requests.latency.p95_ms ?? "—") + " ms";
    $("m-hit").textContent = (m.cache.hit_rate * 100).toFixed(1) + "%";
    $("m-sreq").textContent = m.requests.suggest_requests;
    $("m-search").textContent = m.batch_writer.searches_received;
    $("m-writes").textContent = m.batch_writer.db_upserts;
    $("m-reduce").textContent = m.batch_writer.write_reduction_ratio
      ? m.batch_writer.write_reduction_ratio.toFixed(1) + "×" : "—";
  } catch (_) { /* keep last values */ }
}

// ---- mode toggle ----
document.querySelectorAll(".mode-btn").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".mode-btn").forEach((b) => {
      b.classList.remove("active"); b.setAttribute("aria-checked", "false");
    });
    btn.classList.add("active"); btn.setAttribute("aria-checked", "true");
    mode = btn.dataset.mode;
    setStatus(`Ranking mode: ${mode}`);
    if (input.value.trim()) fetchSuggestions();
  });
});

// ---- events ----
input.addEventListener("input", onInput);
input.addEventListener("keydown", onKeyDown);
input.addEventListener("focus", () => { if (suggestions.length) openBox(); });
searchBtn.addEventListener("click", () => submitSearch(input.value));

box.addEventListener("click", (e) => {
  const li = e.target.closest("li[data-i]");
  if (!li) return;
  const s = suggestions[Number(li.dataset.i)];
  if (s) submitSearch(s.query);
});
box.addEventListener("mousemove", (e) => {
  const li = e.target.closest("li[data-i]");
  if (li) { activeIndex = Number(li.dataset.i); document.querySelectorAll(".suggestions li").forEach((x) => x.classList.remove("active")); li.classList.add("active"); }
});

trendingList.addEventListener("click", (e) => {
  const q = e.target.closest(".t-query");
  if (q) { input.value = q.dataset.q; submitSearch(q.dataset.q); }
});

$("refresh-trending").addEventListener("click", loadTrending);
$("refresh-metrics").addEventListener("click", loadMetrics);

document.addEventListener("click", (e) => {
  if (!e.target.closest("#searchbox")) closeBox();
});

// ---- init ----
// Load the panels once on startup. We deliberately do NOT poll on a timer:
// trending only changes when a search is submitted (which refreshes the panels,
// see submitSearch), and the manual ↻ buttons cover on-demand refresh. This
// keeps the network quiet — no background requests when the user is idle.
loadTrending();
loadMetrics();
input.focus();
