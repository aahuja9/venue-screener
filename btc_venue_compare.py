#!/usr/bin/env python3
"""RISEx slippage screener -- one venue, every market that matters.

Simulates market orders of $10k / $50k / $100k / $500k / $1M against the live
RISEx book on both sides, computes the impact price (fill VWAP) and reports
slippage from mid in bps. Four tabs: Live (pairs x sizes matrix), Liquidity
(resting depth within 1 / 2.5 / 5 bp, EMA-smoothed), Over time (one line per
pair) and Percentiles (P25/50/75/99 per size).

Run:  python3 btc_venue_compare.py     # -> http://localhost:8900
"""
import gzip
import json
import math
import os
import sqlite3
import statistics
import threading
import time
import urllib.request
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

PORT = 8900
DB_PATH = "risex_samples.db"  # durable sample store (survives restarts)

# RISEx market ids, from https://api.rise.trade/v1/markets
MARKET_IDS = {"BTC": 1, "ETH": 2, "SOL": 4, "HYPE": 5, "XAU": 17,
              "SPY": 27, "QQQ": 26, "SNDK": 21}
PAIRS = ["BTC", "ETH", "SOL", "HYPE", "XAU", "SPY", "QQQ", "SNDK"]
# Asset-class sections shown in the UI pair selector.
ASSET_CLASSES = {
    "Crypto": ["BTC", "ETH", "SOL", "HYPE"],
    "Commodities": ["XAU"],
    "Equities": ["SPY", "QQQ", "SNDK"],
}
NOTIONALS = [10_000, 50_000, 100_000, 500_000, 1_000_000]  # USD trade sizes to simulate
DEPTH_BPS = [1.0, 2.5, 5.0]  # resting-liquidity buckets, distance from mid in bps
DEPTH_EMA_TAU = 30.0         # EMA time constant (seconds) for the liquidity matrix
SAMPLE_SECONDS = 5
STATS_WINDOW_MIN = 10   # window for the median columns in the Live tab
HISTORY_MIN = 60        # retention for the over-time charts
BOOK_LEVELS = 500       # deep enough that a $1M order rarely runs out of book

TAKER_FEE_BPS = 3.0     # RISEx base schedule: 1bp maker / 3bp taker


def http_json(url: str, body: dict | None = None, headers: dict | None = None) -> dict:
    hdrs = {"User-Agent": "Mozilla/5.0", "Accept-Encoding": "gzip"}
    if headers:
        hdrs.update(headers)
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        hdrs["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=hdrs)
    with urllib.request.urlopen(req, timeout=12) as r:
        raw = r.read()
        if r.headers.get("Content-Encoding") == "gzip" or raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
    return json.loads(raw)


def book_risex(pair):
    """(bids, asks) as (price, qty) sorted best-first."""
    d = http_json(f"https://api.rise.trade/v1/orderbook"
                  f"?market_id={MARKET_IDS[pair]}&limit={BOOK_LEVELS}")["data"]
    bids = [(float(l["price"]), float(l["quantity"])) for l in d["bids"]]
    asks = [(float(l["price"]), float(l["quantity"])) for l in d["asks"]]
    return bids, asks


# history[pair] = deque of (ts, {notional: (buy_bps, sell_bps)})
_history: dict = {p: deque() for p in PAIRS}
_history_lock = threading.Lock()

# Resting-liquidity EMA for the Liquidity tab, in memory only (not persisted).
# _depth_ema[pair] = {"1": [bid_usd, ask_usd], "2.5": [...], "5": [...]}
_depth_ema: dict = {}
_depth_n: dict = {}   # samples folded into each EMA, so the UI can flag warmup
_depth_lock = threading.Lock()


def walk_book(levels: list, notional_usd: float) -> float | None:
    """Impact price (fill VWAP) for a market order consuming `notional_usd`.
    Returns None if the book can't fill it."""
    remaining = notional_usd
    filled_base = 0.0
    for price, qty in levels:
        level_notional = price * qty
        if level_notional >= remaining:
            filled_base += remaining / price
            remaining = 0.0
            break
        filled_base += qty
        remaining -= level_notional
    if remaining > 0 or filled_base == 0:
        return None
    return notional_usd / filled_base


def book_depth(bids, asks, mid) -> dict:
    """USD notional resting within N bps of mid, per side, for each DEPTH_BPS bucket.
    Buckets are cumulative: the 5bp figure includes everything inside 1bp."""
    out = {}
    for bps in DEPTH_BPS:
        lo, hi = mid * (1 - bps / 1e4), mid * (1 + bps / 1e4)
        out[f"{bps:g}"] = [sum(p * q for p, q in bids if p >= lo),
                           sum(p * q for p, q in asks if p <= hi)]
    return out


def pair_impact(pair) -> dict:
    bids, asks = book_risex(pair)
    if not bids or not asks:
        return {"error": "empty book"}
    best_bid, best_ask = bids[0][0], asks[0][0]
    mid = (best_bid + best_ask) / 2
    sims = []
    for n in NOTIONALS:
        buy_px = walk_book(asks, n)    # buying walks the ask side
        sell_px = walk_book(bids, n)   # selling walks the bid side
        sims.append({
            "notional": n,
            "buy_bps": (buy_px - mid) / mid * 1e4 if buy_px else None,
            "sell_bps": (mid - sell_px) / mid * 1e4 if sell_px else None,
        })
    return {"mid": mid, "spread_bps": (best_ask - best_bid) / mid * 1e4, "sims": sims,
            "depth": book_depth(bids, asks, mid),
            "levels": [len(bids), len(asks)]}


def fetch_all() -> dict:
    """Every pair at once, in parallel."""
    from concurrent.futures import ThreadPoolExecutor

    def one(pair):
        try:
            return pair, pair_impact(pair)
        except Exception as e:
            return pair, {"error": f"{type(e).__name__}: {e}"}

    with ThreadPoolExecutor(max_workers=len(PAIRS)) as pool:
        return dict(pool.map(one, PAIRS))


def update_depth_ema(pair, depth, dt):
    """Fold one sample into the per-pair liquidity EMA (time constant DEPTH_EMA_TAU).
    Pairs that error on a cycle are skipped, so a failed fetch never decays the
    average toward zero."""
    alpha = 1 - math.exp(-dt / DEPTH_EMA_TAU)
    with _depth_lock:
        prev = _depth_ema.get(pair)
        if prev is None:
            _depth_ema[pair] = {k: list(v) for k, v in depth.items()}
            _depth_n[pair] = 1
            return
        for k, (bid, ask) in depth.items():
            pb, pa = prev.get(k, (bid, ask))
            prev[k] = [pb + alpha * (bid - pb), pa + alpha * (ask - pa)]
        _depth_n[pair] = _depth_n.get(pair, 0) + 1


def depth_snapshot() -> dict:
    with _depth_lock:
        return {p: {"depth": {k: list(v) for k, v in _depth_ema[p].items()},
                    "n": _depth_n.get(p, 0)}
                for p in PAIRS if p in _depth_ema}


def window_stats(pair) -> dict:
    cutoff = time.time() - STATS_WINDOW_MIN * 60
    with _history_lock:
        samples = [(t, pt) for t, pt in _history[pair] if t >= cutoff]
    out = {"n": len(samples), "sims": {}}
    for n in NOTIONALS:
        buys = [pt[n][0] for _, pt in samples if n in pt and pt[n][0] is not None]
        sells = [pt[n][1] for _, pt in samples if n in pt and pt[n][1] is not None]
        if buys or sells:
            out["sims"][str(n)] = {
                "buy_median": statistics.median(buys) if buys else None,
                "sell_median": statistics.median(sells) if sells else None,
            }
    return out


def history_series() -> dict:
    out = {}
    with _history_lock:
        for pair in PAIRS:
            q = _history[pair]
            entry = {"t": [round(t) for t, _ in q]}
            for n in NOTIONALS:
                entry[str(n)] = {
                    "buy": [pt.get(n, (None, None))[0] for _, pt in q],
                    "sell": [pt.get(n, (None, None))[1] for _, pt in q],
                }
            out[pair] = entry
    return out


def _pctl(sorted_vals, q):
    if not sorted_vals:
        return None
    idx = q * (len(sorted_vals) - 1)
    lo, hi = int(idx), min(int(idx) + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


_QS = (("p25", .25), ("p50", .5), ("p75", .75), ("p99", .99))


def percentiles(pair: str, minutes: int) -> dict:
    cutoff = int(time.time() - minutes * 60)
    con = sqlite3.connect(DB_PATH)
    try:
        rows = con.execute(
            "SELECT spread_bps, data FROM samples WHERE pair=? AND ts>=?",
            (pair, cutoff)).fetchall()
    finally:
        con.close()
    spreads = sorted(r[0] for r in rows if r[0] is not None)
    points = [json.loads(r[1]) for r in rows]
    out = {"n": len(rows), "minutes": minutes,
           "spread": {q: _pctl(spreads, p) for q, p in _QS},
           "sizes": []}
    for n in NOTIONALS:
        k = str(n)
        buys = sorted(pt[k][0] for pt in points if k in pt and pt[k][0] is not None)
        sells = sorted(pt[k][1] for pt in points if k in pt and pt[k][1] is not None)
        total = sum(1 for pt in points if k in pt)
        out["sizes"].append({
            "notional": n,
            "buy": {q: _pctl(buys, p) for q, p in _QS},
            "sell": {q: _pctl(sells, p) for q, p in _QS},
            "fill_rate": (min(len(buys), len(sells)) / total * 100) if total else None,
        })
    return out


def _init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS samples (
        ts INTEGER NOT NULL, pair TEXT NOT NULL,
        spread_bps REAL, data TEXT NOT NULL)""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_samples ON samples (pair, ts)")
    con.commit()
    con.close()


def _load_history():
    """Rehydrate the in-memory chart window from sqlite so restarts don't wipe it."""
    cutoff = int(time.time() - HISTORY_MIN * 60)
    con = sqlite3.connect(DB_PATH)
    try:
        rows = con.execute(
            "SELECT ts, pair, data FROM samples WHERE ts >= ? ORDER BY ts", (cutoff,)).fetchall()
    finally:
        con.close()
    for ts, pair, data in rows:
        if pair in _history:
            _history[pair].append((ts, {int(k): tuple(v) for k, v in json.loads(data).items()}))


def _sampler():
    last_cycle = None
    con = sqlite3.connect(DB_PATH)
    while True:
        now = time.time()
        # Real elapsed time drives the EMA weight, clamped so a stalled cycle
        # (or a laptop resuming from sleep) can't blow the average away.
        dt = SAMPLE_SECONDS if last_cycle is None else min(120.0, max(1.0, now - last_cycle))
        last_cycle = now
        batch = []
        for pair, d in fetch_all().items():
            if "depth" in d:
                update_depth_ema(pair, d["depth"], dt)
            if "sims" in d:
                point = {s["notional"]: (s["buy_bps"], s["sell_bps"]) for s in d["sims"]}
                with _history_lock:
                    _history[pair].append((now, point))
                batch.append((int(now), pair, d.get("spread_bps"),
                              json.dumps({str(k): v for k, v in point.items()})))
        if batch:
            con.executemany("INSERT INTO samples VALUES (?,?,?,?)", batch)
            con.commit()
        cutoff = now - HISTORY_MIN * 60
        with _history_lock:
            for q in _history.values():
                while q and q[0][0] < cutoff:
                    q.popleft()
        # target a fixed cadence: subtract however long this cycle took
        time.sleep(max(0.5, SAMPLE_SECONDS - (time.time() - now)))


PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><title>RISEx Slippage Screener</title>
<style>
.viz-root, body { /* palette roles (reference palette; light + dark selected) */
  --surface-1:#fcfcfb; --text-primary:#0b0b0b; --text-secondary:#52514e; --line:#e3e4ee;
  --card:#ffffff; --hl:#eef;
  --s-btc:#d97706; --s-eth:#4a3aa7; --s-sol:#0d9488; --s-hype:#008300;
  --s-xau:#b45309; --s-spy:#2a78d6; --s-qqq:#c026d3; --s-sndk:#e34948;
  --buy:#0e7a4f; --sell:#b3372f;
}
@media (prefers-color-scheme: dark) {
  .viz-root, body {
    --surface-1:#1a1a19; --text-primary:#ffffff; --text-secondary:#c3c2b7; --line:#2a2a3a;
    --card:#1e1e28; --hl:#22223a;
    --s-btc:#f59e0b; --s-eth:#9085e9; --s-sol:#2dd4bf; --s-hype:#22c55e;
    --s-xau:#eab308; --s-spy:#3987e5; --s-qqq:#d946ef; --s-sndk:#e66767;
    --buy:#3ecf8e; --sell:#ff7a70;
  }
}
* { box-sizing:border-box; margin:0 }
body { font:14px/1.45 -apple-system, system-ui, sans-serif; color:var(--text-primary); background:var(--surface-1); padding:24px }
h1 { font-size:18px; margin-bottom:4px }
.sub { color:var(--text-secondary); font-size:12px; margin-bottom:18px }
.controls { display:flex; align-items:center; gap:10px; margin-bottom:16px; flex-wrap:wrap }
.controls button, .seg button { font-size:14px; border:1px solid var(--line); background:var(--card);
  color:var(--text-primary); border-radius:8px; cursor:pointer; padding:6px 12px }
.controls .step { font-size:16px; width:34px }
.controls .val { font-variant-numeric:tabular-nums; min-width:104px; text-align:center;
  border:1px solid var(--line); background:var(--card); border-radius:8px; padding:7px 10px }
.seg { display:inline-flex; gap:0; border:1px solid var(--line); border-radius:8px; overflow:hidden }
.seg button { border:none; border-radius:0; border-right:1px solid var(--line) }
.seg button:last-child { border-right:none }
.seg button.on { background:var(--hl); font-weight:600 }
.tabs { display:flex; gap:8px; margin-bottom:16px; align-items:center; flex-wrap:wrap }
.tabs button { padding:8px 18px; font-size:14px; border:1px solid var(--line); border-radius:8px;
  background:var(--card); color:var(--text-primary); cursor:pointer }
.tabs button.on { background:var(--hl); font-weight:600 }
.card { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:16px 18px; margin-bottom:16px }
.card h2 { font-size:15px; margin-bottom:10px }
.scroll { overflow-x:auto }
table { width:100%; border-collapse:collapse; font-variant-numeric:tabular-nums }
th { color:var(--text-secondary); font-size:11px; text-transform:uppercase; letter-spacing:.04em; text-align:right; padding:4px 8px; white-space:nowrap }
th:first-child { text-align:left }
th.grp { text-align:center; border-bottom:1px solid var(--line) }
td { padding:7px 8px; text-align:right; border-top:1px solid var(--line); white-space:nowrap }
td:first-child { text-align:left; font-weight:600 }
.buyv { color:var(--buy) } .sellv { color:var(--sell) }
.best { background:var(--hl); border-radius:4px }
.mut { color:var(--text-secondary); font-weight:400 } .err { color:var(--sell); font-weight:400; font-size:12px }
.stamp { color:var(--text-secondary); font-size:12px; margin-top:8px }
.legend { display:flex; gap:16px; flex-wrap:wrap; font-size:12px; color:var(--text-secondary); margin-bottom:6px }
.chip { display:inline-block; width:10px; height:10px; border-radius:3px; margin-right:5px; vertical-align:-1px }
.chartwrap { position:relative }
.tooltip { position:absolute; pointer-events:none; background:var(--card); border:1px solid var(--line);
  border-radius:8px; padding:8px 10px; font-size:12px; display:none; z-index:5; min-width:150px;
  box-shadow:0 4px 14px rgba(0,0,0,.15) }
.tooltip .trow { display:flex; justify-content:space-between; gap:12px }
svg text { fill:var(--text-secondary); font-size:11px; font-variant-numeric:tabular-nums }
svg .grid { stroke:var(--line); stroke-width:1 }
svg .xline { stroke:var(--text-secondary); stroke-width:1; opacity:.5 }
</style></head><body class="viz-root">
<h1>RISEx slippage screener</h1>
<div class="sub">simulated market orders ($10k / $50k / $100k / $500k / $1M) against the live RISEx book &middot;
impact price = fill VWAP walking the book &middot; slippage = distance from RISEx's own mid, in bps &middot;
both sides shown: <span class="buyv">buy</span> lifts the asks, <span class="sellv">sell</span> hits the bids &middot;
sampled every 5s server-side, 60 min retained &middot; taker fee 3bp (not included)</div>
<div class="tabs">
  <button id="tab-live" class="on">Live</button>
  <button id="tab-liq">Liquidity</button>
  <button id="tab-time">Over time</button>
  <button id="tab-perc">Percentiles</button>
</div>
<div class="tabs" id="pairFilter" style="margin-top:-8px;gap:6px"></div>

<div id="view-live">
  <div class="controls">
    <button class="step" id="dec" title="refresh more often">&minus;</button>
    <div class="val"><span id="ival">500ms</span> refresh</div>
    <button class="step" id="inc">+</button>
    <button id="pause">Pause</button>
    <span id="status" class="sub" style="margin:0"></span>
  </div>
  <div class="card"><h2>Live slippage from mid (bps)</h2><div class="scroll" id="live-table"></div>
    <div class="mut" style="font-size:12px;margin-top:8px">Best (lowest) figure in each column is highlighted.
    &quot;n/a&quot; means the visible book could not fill that size on that side.</div></div>
  <div class="card"><h2>Median over the last 10 minutes (bps)</h2><div class="scroll" id="med-table"></div></div>
</div>

<div id="view-liq" style="display:none">
  <div class="controls">
    <span class="mut">side</span>
    <span class="seg" id="selSide">
      <button data-side="both" class="on">Bid + ask</button><button data-side="bid">Bid</button><button data-side="ask">Ask</button>
    </span>
  </div>
  <div class="card"><h2>Resting liquidity within N bps of mid &mdash; USD notional</h2><div class="scroll" id="liq-table"></div>
    <div class="mut" style="font-size:12px;margin-top:8px">Sum of resting orders inside each distance from mid, so the buckets are
    cumulative (5bp includes everything inside 1bp). Smoothed server-side as an exponential moving average with a __DEPTH_TAU__s time
    constant, recomputed once per 5s sample; the page re-reads it every 5s. Higher is better. Pairs still warming up (fewer than one
    full time constant of samples) are marked with a dot.</div></div>
</div>

<div id="view-time" style="display:none">
  <div class="controls">
    <span class="mut">size</span>
    <span class="seg" id="selN"></span>
    <span class="mut">window</span>
    <span class="seg" id="selW">
      <button data-w="10" class="on">10m</button><button data-w="30">30m</button><button data-w="60">60m</button>
    </span>
    <span class="mut">smoothing</span>
    <span class="seg" id="selS">
      <button data-s="raw">Raw</button><button data-s="ema">EMA 3m</button><button data-s="med" class="on">Median 5m</button>
    </span>
  </div>
  <div class="card"><h2>Buy side &mdash; slippage from mid (bps, log scale)</h2>
    <div class="legend" id="legend-buy"></div>
    <div class="chartwrap"><svg id="chart-buy" width="100%" height="260"></svg><div class="tooltip" id="tip-buy"></div></div></div>
  <div class="card"><h2>Sell side &mdash; slippage from mid (bps, log scale)</h2>
    <div class="legend" id="legend-sell"></div>
    <div class="chartwrap"><svg id="chart-sell" width="100%" height="260"></svg><div class="tooltip" id="tip-sell"></div></div></div>
</div>

<div id="view-perc" style="display:none">
  <div class="controls">
    <span class="mut">pair</span>
    <span class="seg" id="selP">__PAIR_SELECTOR__</span>
    <span class="mut">timeframe</span>
    <span class="seg" id="selT">
      <button data-m="10">10m</button><button data-m="30">30m</button><button data-m="60" class="on">1h</button>
      <button data-m="180">3h</button><button data-m="720">12h</button><button data-m="1440">24h</button><button data-m="4320">3d</button><button data-m="10080">7d</button>
    </span>
  </div>
  <div class="card"><h2 id="perc-title">Slippage percentiles</h2><div class="scroll" id="perc-table"></div>
    <div class="mut" style="font-size:12px;margin-top:8px">bps from mid; lower is better. fill% = share of samples where the book could
    fill the size on both sides. Data persists to disk from when the collector first ran, so long timeframes fill in over time.</div></div>
</div>
<div class="stamp" id="stamp"></div>

<script>
let intervalSec = 0.5, timer = null, paused = false, inflight = false;
let curTab = "live", curN = 10000, curW = 10, curS = "med", lastHistory = null;
let curPair = "BTC", curT = 60, curSide = "both", lastLiq = null, lastLive = null;
const LIQ_REFRESH_MS = 5000;   // the EMA only advances once per 5s sample
const PAIRS = __PAIRS__;
const NOTIONALS = __NOTIONALS__;
const DEPTH_BPS = __DEPTH_BPS__;   // liquidity buckets, as string keys
const DEPTH_TAU = __DEPTH_TAU__;   // EMA time constant, seconds
const COLORS = Object.fromEntries(PAIRS.map(p => [p, "var(--s-" + p.toLowerCase() + ")"]));

// Pair filter: chips below the tabs hide a pair from every matrix and chart.
let hidden = [];
try { hidden = JSON.parse(localStorage.getItem("pairFilter") || "[]"); } catch (e) {}
const isHidden = p => hidden.includes(p);
const shown = () => PAIRS.filter(p => !isHidden(p));
function togglePair(p) {
  const i = hidden.indexOf(p);
  i >= 0 ? hidden.splice(i, 1) : hidden.push(p);
  try { localStorage.setItem("pairFilter", JSON.stringify(hidden)); } catch (e) {}
  renderPairFilter();
  if (lastLive) renderLive(lastLive);
  if (lastLiq) renderLiq(lastLiq);
  if (lastHistory) renderTime();
}
function renderPairFilter() {
  const el = document.getElementById("pairFilter");
  el.innerHTML = `<span class="mut" style="font-size:12px">pairs</span>` + PAIRS.map(p =>
    `<button data-p="${p}" style="padding:4px 10px;font-size:12px;${isHidden(p) ? "opacity:.35" : ""}" ` +
    `title="${isHidden(p) ? "click to show" : "click to hide"} ${p}">` +
    `<span class="chip" style="background:${COLORS[p]}"></span> ${p}</button>`).join("");
  for (const b of el.querySelectorAll("button")) b.onclick = () => togglePair(b.dataset.p);
}

const fmtMid = v => v.toLocaleString(undefined, {maximumSignificantDigits: 7});
const fmtBps = v => (v === null || v === undefined) ? "n/a" : v.toFixed(2);
const fmtN = n => n >= 1e6 ? "$" + (n/1e6) + "M" : "$" + (n/1000) + "k";
const fmtUsd = v => {
  if (v === null || v === undefined) return "-";
  if (v >= 1e9) return "$" + (v/1e9).toFixed(2) + "B";
  if (v >= 1e6) return "$" + (v/1e6).toFixed(2) + "M";
  if (v >= 1e3) return "$" + (v/1e3).toFixed(1) + "k";
  return "$" + v.toFixed(0);
};

/* ---------- Live tab ---------- */
// One matrix: rows = pairs, a buy/sell column pair per order size.
function matrix(data, pick) {
  const rows = shown().filter(p => data[p]);
  const best = {};   // lowest (best) value per column, for highlighting
  for (const n of NOTIONALS) for (const side of ["buy", "sell"]) {
    let m = null;
    for (const p of rows) {
      const v = pick(data[p], n, side);
      if (v !== null && v !== undefined && (m === null || v < m)) m = v;
    }
    best[n + side] = m;
  }
  const hasMid = rows.some(p => data[p].mid !== undefined);
  const body = rows.map(p => {
    const d = data[p];
    if (d.error) return `<tr><td><span class="chip" style="background:${COLORS[p]}"></span> ${p}</td>
      <td colspan="${NOTIONALS.length * 2 + (hasMid ? 2 : 0)}" class="err">${d.error}</td></tr>`;
    const cells = NOTIONALS.map(n => ["buy", "sell"].map(side => {
      const v = pick(d, n, side);
      const cls = side === "buy" ? "buyv" : "sellv";
      const hit = v !== null && v !== undefined && v === best[n + side] ? " best" : "";
      return `<td class="${cls}${hit}">${fmtBps(v)}</td>`;
    }).join("")).join("");
    const mid = d.mid === undefined ? "" : `<td class="mut">${fmtMid(d.mid)}</td><td class="mut">${d.spread_bps.toFixed(3)}</td>`;
    return `<tr><td><span class="chip" style="background:${COLORS[p]}"></span> ${p}</td>${mid}${cells}</tr>`;
  }).join("");
  return `<table>
    <tr><th rowspan="2">pair</th>${hasMid ? '<th rowspan="2">mid</th><th rowspan="2">spread bp</th>' : ""}
      ${NOTIONALS.map(n => `<th colspan="2" class="grp">${fmtN(n)}</th>`).join("")}</tr>
    <tr>${NOTIONALS.map(() => `<th>buy</th><th>sell</th>`).join("")}</tr>
    ${body}</table>`;
}

function renderLive(data) {
  document.getElementById("live-table").innerHTML =
    matrix(data, (d, n, side) => {
      const s = (d.sims || []).find(x => x.notional === n);
      return s ? s[side + "_bps"] : null;
    });
  document.getElementById("med-table").innerHTML =
    matrix(Object.fromEntries(Object.entries(data).map(([p, d]) => [p, {stats: d.stats, error: d.error}])),
      (d, n, side) => ((d.stats || {}).sims || {})[String(n)]?.[side + "_median"] ?? null);
}

/* ---------- Liquidity tab ---------- */
function liqValue(d, bps) {
  const v = (d.depth || {})[bps];
  if (!v) return null;
  return curSide === "bid" ? v[0] : curSide === "ask" ? v[1] : v[0] + v[1];
}
function renderLiq(data) {
  const rows = shown().filter(p => data[p]);
  const best = {};
  for (const b of DEPTH_BPS) best[b] = Math.max(-1, ...rows.map(p => liqValue(data[p], b) ?? -1));
  const body = rows.map(p => {
    const d = data[p];
    const cells = DEPTH_BPS.map(b => {
      const v = liqValue(d, b);
      return `<td class="${v !== null && v === best[b] ? "best" : ""}">${fmtUsd(v)}</td>`;
    }).join("");
    const warming = (d.n || 0) < DEPTH_TAU / 5;   // less than one time constant of samples
    return `<tr><td><span class="chip" style="background:${COLORS[p]}"></span> ${p}` +
      (warming ? ` <span class="mut" title="EMA still warming up (${d.n} samples)">&middot;</span>` : "") +
      `</td>${cells}</tr>`;
  }).join("");
  document.getElementById("liq-table").innerHTML =
    `<table><tr><th>pair</th>` + DEPTH_BPS.map(b => `<th>within ${b} bp</th>`).join("") +
    `</tr>${body}</table>`;
}

/* ---------- Over-time tab ---------- */
function smoothSeries(pts, mode) {
  if (mode === "raw") return pts;
  const out = [];
  if (mode === "med") {
    const W = 300;  // 5-minute trailing median
    for (let i = 0; i < pts.length; i++) {
      const t = pts[i][0], win = [];
      for (let j = i; j >= 0 && pts[j][0] >= t - W; j--)
        if (pts[j][1] !== null && pts[j][1] > 0) win.push(pts[j][1]);
      if (!win.length) { out.push([t, null]); continue; }
      win.sort((a, b) => a - b);
      const m = win.length % 2 ? win[(win.length-1)/2] : (win[win.length/2-1] + win[win.length/2]) / 2;
      out.push([t, m]);
    }
  } else {
    const HL = 180;  // EMA with 3-minute half-life, time-aware
    let ema = null, lastT = null;
    for (const [t, y] of pts) {
      if (y === null || y <= 0) { out.push([t, ema]); continue; }
      if (ema === null) ema = y;
      else ema = ema + (1 - Math.pow(0.5, (t - lastT) / HL)) * (y - ema);
      lastT = t;
      out.push([t, ema]);
    }
  }
  return out;
}

function drawChart(svgId, tipId, hist, side) {
  const svg = document.getElementById(svgId);
  const W = svg.clientWidth || 800, H = 260, padL = 46, padR = 12, padT = 10, padB = 22;
  const now = Date.now() / 1000, t0 = now - curW * 60;
  let vals = [];
  const rawSeries = {}, series = {};
  for (const p of PAIRS) {
    if (isHidden(p)) continue;
    const h = hist[p]; if (!h) continue;
    const pts = [];
    for (let i = 0; i < h.t.length; i++) {
      if (h.t[i] < t0) continue;
      pts.push([h.t[i], h[String(curN)][side][i]]);
    }
    rawSeries[p] = pts;
    series[p] = smoothSeries(pts, curS);
    for (const [, y] of series[p]) if (y !== null && y > 0) vals.push(y);
  }
  if (!vals.length) { svg.innerHTML = "<text x='20' y='40'>collecting samples...</text>"; return; }
  const yMin = Math.max(0.003, Math.min(...vals) * 0.8), yMax = Math.max(...vals) * 1.25;
  const lg = Math.log10;
  const X = t => padL + (t - t0) / (now - t0) * (W - padL - padR);
  const Y = y => padT + (1 - (lg(y) - lg(yMin)) / (lg(yMax) - lg(yMin))) * (H - padT - padB);
  let g = "";
  for (let e = Math.ceil(lg(yMin)); e <= Math.floor(lg(yMax)); e++) {
    const y = Math.pow(10, e);
    g += `<line class="grid" x1="${padL}" y1="${Y(y)}" x2="${W-padR}" y2="${Y(y)}"/>`;
    g += `<text x="${padL-6}" y="${Y(y)+4}" text-anchor="end">${y >= 1 ? y : y.toFixed(2)}</text>`;
  }
  for (let i = 0; i <= 3; i++) {
    const t = t0 + (now - t0) * i / 3;
    const d = new Date(t * 1000);
    g += `<text x="${X(t)}" y="${H-6}" text-anchor="middle">${d.toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'})}</text>`;
  }
  const pathFor = pts => {
    let dstr = "", pen = false;
    for (const [t, y] of pts) {
      if (y === null || y <= 0 || y < yMin || y > yMax) { pen = false; continue; }
      dstr += (pen ? "L" : "M") + X(t).toFixed(1) + "," + Y(y).toFixed(1);
      pen = true;
    }
    return dstr;
  };
  if (curS !== "raw") {
    for (const p of PAIRS) {
      const dstr = pathFor(rawSeries[p] || []);
      if (dstr) g += `<path d="${dstr}" fill="none" stroke="${COLORS[p]}" stroke-width="1" opacity="0.22"/>`;
    }
  }
  for (const p of PAIRS) {
    const dstr = pathFor(series[p] || []);
    if (dstr) g += `<path d="${dstr}" fill="none" stroke="${COLORS[p]}" stroke-width="2" stroke-linejoin="round"/>`;
  }
  g += `<line id="${svgId}-x" class="xline" y1="${padT}" y2="${H-padB}" x1="-10" x2="-10"/>`;
  svg.innerHTML = g;

  const tip = document.getElementById(tipId);
  svg.onmousemove = e => {
    const r = svg.getBoundingClientRect();
    const mx = e.clientX - r.left;
    const t = t0 + (mx - padL) / (W - padL - padR) * (now - t0);
    if (t < t0 || t > now) { tip.style.display = "none"; return; }
    let entries = [], xline = null;
    for (const p of PAIRS) {
      const pts = series[p] || [];
      if (!pts.length) continue;
      let best = null;
      for (const q of pts) if (!best || Math.abs(q[0]-t) < Math.abs(best[0]-t)) best = q;
      if (!best || Math.abs(best[0]-t) > 30) continue;
      xline = best[0];
      entries.push([p, best[1]]);
    }
    if (!entries.length) { tip.style.display = "none"; return; }
    entries.sort((a, b) => (a[1] === null) - (b[1] === null) || a[1] - b[1]);
    const rows = entries.map(([p, val], i) =>
      `<div class="trow"><span><span class="mut">${i+1}.</span> <span class="chip" style="background:${COLORS[p]}"></span>${p}</span><b>${fmtBps(val)}</b></div>`).join("");
    const xl = document.getElementById(svgId + "-x");
    if (xl && xline) { xl.setAttribute("x1", X(xline)); xl.setAttribute("x2", X(xline)); }
    tip.innerHTML = `<div class="mut" style="margin-bottom:4px">${new Date((xline||t)*1000).toLocaleTimeString()} &middot; ${fmtN(curN)} ${side}</div>` + rows;
    tip.style.display = "block";
    tip.style.left = Math.min(mx + 14, W - 180) + "px";
    tip.style.top = "14px";
  };
  svg.onmouseleave = () => { tip.style.display = "none"; };

  // Legend ranked best -> worst by latest smoothed slippage for THIS side.
  const legendId = svgId === "chart-buy" ? "legend-buy" : "legend-sell";
  const ranked = [];
  for (const p of PAIRS) {
    if (isHidden(p)) continue;
    const pts = series[p] || [];
    let last = null;
    for (let i = pts.length - 1; i >= 0; i--)
      if (pts[i][1] !== null && pts[i][1] > 0) { last = pts[i][1]; break; }
    ranked.push([p, last]);
  }
  ranked.sort((a, b) => (a[1] === null) - (b[1] === null) || a[1] - b[1]);
  const items = ranked.map(([p, val], i) =>
    `<span class="legitem" data-p="${p}" style="cursor:pointer" title="click to hide">` +
    `<span class="mut">${i+1}.</span> <span class="chip" style="background:${COLORS[p]}"></span>${p}` +
    ` <b>${val === null ? "n/a" : val.toFixed(2)}</b></span>`);
  for (const p of PAIRS) if (isHidden(p))
    items.push(`<span class="legitem" data-p="${p}" style="cursor:pointer;opacity:.35" title="click to show">` +
      `<span class="chip" style="background:${COLORS[p]}"></span>${p}</span>`);
  document.getElementById(legendId).innerHTML = items.join("");
  for (const el of document.querySelectorAll(`#${legendId} .legitem`))
    el.onclick = () => togglePair(el.dataset.p);
}

function renderTime() {
  if (!lastHistory) return;
  drawChart("chart-buy", "tip-buy", lastHistory, "buy");
  drawChart("chart-sell", "tip-sell", lastHistory, "sell");
}

/* ---------- Percentiles tab ---------- */
function renderPerc(d) {
  document.getElementById("perc-title").textContent =
    `${curPair} — slippage percentiles, last ${curT >= 60 ? (curT/60)+"h" : curT+"m"} (${d.n} samples)`;
  const P = ["p25","p50","p75","p99"];
  const f = v => (v === null || v === undefined) ? "n/a" : v.toFixed(2);
  let html = `<table><tr><th>size</th>${P.map(p=>`<th>buy ${p}</th>`).join("")}${P.map(p=>`<th>sell ${p}</th>`).join("")}<th>fill%</th></tr>`;
  html += `<tr><td>spread</td>${P.map(p=>`<td class="mut">${f(d.spread[p])}</td>`).join("")}<td colspan="5" class="mut">quoted spread, bps</td></tr>`;
  for (const s of d.sizes) {
    html += `<tr><td>${fmtN(s.notional)}</td>` +
      P.map(p=>`<td class="buyv">${f(s.buy[p])}</td>`).join("") +
      P.map(p=>`<td class="sellv">${f(s.sell[p])}</td>`).join("") +
      `<td>${s.fill_rate === null ? "n/a" : s.fill_rate.toFixed(0)}</td></tr>`;
  }
  document.getElementById("perc-table").innerHTML = html + "</table>";
}

/* ---------- data loop ---------- */
async function tick() {
  if (paused || inflight) return;
  inflight = true;
  document.getElementById("status").textContent = "fetching...";
  try {
    if (curTab === "live") {
      lastLive = await (await fetch("/live")).json();
      renderLive(lastLive);
    } else if (curTab === "liq") {
      lastLiq = await (await fetch("/liquidity")).json();
      renderLiq(lastLiq);
    } else if (curTab === "time") {
      lastHistory = await (await fetch("/history")).json();
      renderTime();
    } else {
      renderPerc(await (await fetch(`/percentiles?pair=${curPair}&minutes=${curT}`)).json());
    }
    document.getElementById("stamp").textContent = "last update " + new Date().toLocaleTimeString() +
      " - RISEx - lower is better; slippage includes half the spread since it is measured from mid";
    document.getElementById("status").textContent = "";
  } catch (e) {
    document.getElementById("status").textContent = "fetch failed: " + e;
  } finally { inflight = false; }
}

// The Liquidity tab runs on its own fixed 5s cadence; every other tab follows
// the refresh control.
function arm() {
  clearInterval(timer);
  timer = setInterval(tick, curTab === "liq" ? LIQ_REFRESH_MS : intervalSec*1000);
}
function fmtIval(s) { return s < 1 ? (s*1000) + "ms" : s + "s"; }
function setInterval_(s) {
  intervalSec = Math.min(120, Math.max(0.5, s));
  document.getElementById("ival").textContent = fmtIval(intervalSec);
  arm();
}
function stepFor(s, dir) {
  if (dir < 0) return s > 10 ? 5 : (s > 2 ? 1 : 0.5);
  return s >= 10 ? 5 : (s >= 2 ? 1 : 0.5);
}
document.getElementById("dec").onclick = () => setInterval_(intervalSec - stepFor(intervalSec, -1));
document.getElementById("inc").onclick = () => setInterval_(intervalSec + stepFor(intervalSec, +1));
document.getElementById("ival").textContent = fmtIval(intervalSec);
document.getElementById("pause").onclick = e => {
  paused = !paused;
  e.target.textContent = paused ? "Resume" : "Pause";
  if (!paused) tick();
};

function setTab(t) {
  curTab = t;
  for (const [id, key] of [["tab-live","live"],["tab-liq","liq"],["tab-time","time"],["tab-perc","perc"]])
    document.getElementById(id).classList.toggle("on", t === key);
  for (const [id, key] of [["view-live","live"],["view-liq","liq"],["view-time","time"],["view-perc","perc"]])
    document.getElementById(id).style.display = t === key ? "" : "none";
  arm();   // cadence differs per tab
  tick();
}
document.getElementById("tab-live").onclick = () => setTab("live");
document.getElementById("tab-liq").onclick = () => setTab("liq");
document.getElementById("tab-time").onclick = () => setTab("time");
document.getElementById("tab-perc").onclick = () => setTab("perc");

document.getElementById("selSide").onclick = e => {
  if (e.target.dataset.side) {
    curSide = e.target.dataset.side;
    for (const b of document.querySelectorAll("#selSide button")) b.classList.toggle("on", b === e.target);
    if (lastLiq) renderLiq(lastLiq);   // re-render from cache, no refetch
  }
};
document.getElementById("selN").innerHTML = NOTIONALS.map((n, i) =>
  `<button data-n="${n}"${i === 0 ? ' class="on"' : ""}>${fmtN(n)}</button>`).join("");
document.getElementById("selN").onclick = e => {
  if (e.target.dataset.n) {
    curN = Number(e.target.dataset.n);
    for (const b of document.querySelectorAll("#selN button")) b.classList.toggle("on", b === e.target);
    renderTime();
  }
};
document.getElementById("selW").onclick = e => {
  if (e.target.dataset.w) {
    curW = Number(e.target.dataset.w);
    for (const b of document.querySelectorAll("#selW button")) b.classList.toggle("on", b === e.target);
    renderTime();
  }
};
document.getElementById("selS").onclick = e => {
  if (e.target.dataset.s) {
    curS = e.target.dataset.s;
    for (const b of document.querySelectorAll("#selS button")) b.classList.toggle("on", b === e.target);
    renderTime();
  }
};
document.getElementById("selP").onclick = e => {
  if (e.target.dataset.p) {
    curPair = e.target.dataset.p;
    for (const b of document.querySelectorAll("#selP button")) b.classList.toggle("on", b === e.target);
    tick();
  }
};
document.getElementById("selT").onclick = e => {
  if (e.target.dataset.m) {
    curT = Number(e.target.dataset.m);
    for (const b of document.querySelectorAll("#selT button")) b.classList.toggle("on", b === e.target);
    tick();
  }
};
window.addEventListener("resize", renderTime);
renderPairFilter(); tick(); arm();
</script></body></html>"""


DASH_PASSWORD = os.environ.get("DASH_PASSWORD", "")  # set to enable HTTP Basic Auth


class Handler(BaseHTTPRequestHandler):
    def _authorized(self) -> bool:
        if not DASH_PASSWORD:
            return True  # no password configured (local use) -> open
        import base64
        h = self.headers.get("Authorization", "")
        if h.startswith("Basic "):
            try:
                decoded = base64.b64decode(h[6:]).decode()
                return decoded.split(":", 1)[-1] == DASH_PASSWORD
            except Exception:
                return False
        return False

    def do_GET(self):
        if not self._authorized():
            body = b"Authentication required"
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Basic realm="risex-screener"')
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)
        pair = (qs.get("pair") or [PAIRS[0]])[0].upper()
        if pair not in PAIRS:
            pair = PAIRS[0]
        if path == "/":
            sel = "".join(f'<button data-p="{p}"{" class=\"on\"" if p == PAIRS[0] else ""}>{p}</button>'
                          for p in PAIRS)
            body = (PAGE.replace("__PAIRS__", json.dumps(PAIRS))
                        .replace("__NOTIONALS__", json.dumps(NOTIONALS))
                        .replace("__DEPTH_BPS__", json.dumps([f"{b:g}" for b in DEPTH_BPS]))
                        .replace("__DEPTH_TAU__", str(int(DEPTH_EMA_TAU)))
                        .replace("__PAIR_SELECTOR__", sel)).encode()
            ctype = "text/html; charset=utf-8"
        elif path == "/live":
            out = fetch_all()
            for p in out:
                out[p]["stats"] = window_stats(p)
            body = json.dumps(out).encode()
            ctype = "application/json"
        elif path == "/liquidity":
            body = json.dumps(depth_snapshot()).encode()
            ctype = "application/json"
        elif path == "/history":
            body = json.dumps(history_series()).encode()
            ctype = "application/json"
        elif path == "/percentiles":
            minutes = int((qs.get("minutes") or ["60"])[0])
            body = json.dumps(percentiles(pair, minutes)).encode()
            ctype = "application/json"
        else:
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    _init_db()
    _load_history()
    threading.Thread(target=_sampler, daemon=True).start()
    # Deployed (PORT env set, e.g. Render): bind all interfaces. Local: loopback only.
    port = int(os.environ.get("PORT", PORT))
    host = "0.0.0.0" if "PORT" in os.environ else "127.0.0.1"
    print(f"RISEx slippage screener -> http://{host}:{port}")
    ThreadingHTTPServer((host, port), Handler).serve_forever()
