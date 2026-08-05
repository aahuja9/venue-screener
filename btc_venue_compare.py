#!/usr/bin/env python3
"""Perp slippage comparison across venues -- commodities + equities edition.

Pairs: XAU, XAG (commodities) and SNDK, SPCX (the RISEx equity listings).
Simulates market orders on both sides of each venue's book, computes the
impact price (fill VWAP), and reports slippage vs the venue's mid in bps.
Live tab + over-time chart tab. Serves http://localhost:8900.

Run:  python3 btc_venue_compare.py
"""
import gzip
import json
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
DB_PATH = "venue_samples.db"  # durable sample store (survives restarts)
PAIRS = ["XAU", "XAG", "SNDK", "SPCX"]
# Asset-class sections shown in the UI pair selector.
ASSET_CLASSES = {
    "Commodities": ["XAU", "XAG"],
    "Equities": ["SNDK", "SPCX"],
}
NOTIONALS = [10_000, 25_000, 50_000, 100_000, 250_000, 500_000]  # USD trade sizes to simulate
SAMPLE_SECONDS = 5
STATS_WINDOW_MIN = 10   # window for the mean/median columns in the Live tab
HISTORY_MIN = 60        # retention for the over-time charts

# Per-venue market identifiers
RISEX_IDS = {"XAU": 17, "XAG": 18, "SNDK": 21, "SPCX": 22}
LIGHTER_IDS = {"XAU": 92, "XAG": 93, "SNDK": 139, "SPCX": 194}
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


# ---------------- Venue adapters ----------------
# Each takes a pair symbol and returns (bids, asks) as (price, qty) sorted best-first.

def book_risex(pair):
    d = http_json(f"https://api.rise.trade/v1/orderbook?market_id={RISEX_IDS[pair]}&limit=250")["data"]
    bids = [(float(l["price"]), float(l["quantity"])) for l in d["bids"]]
    asks = [(float(l["price"]), float(l["quantity"])) for l in d["asks"]]
    return bids, asks


def book_extended(pair):
    # SNDK trades as SNDK_24_5-USD; SPCX market (XYZSPCX_ORCLPX-USD) is REDUCE_ONLY, excluded.
    sym = {"SNDK": "SNDK_24_5"}.get(pair, pair)
    d = http_json(f"https://api.starknet.extended.exchange/api/v1/info/markets/{sym}-USD/orderbook")["data"]
    bids = sorted(((float(l["price"]), float(l["qty"])) for l in d["bid"]), key=lambda x: -x[0])
    asks = sorted(((float(l["price"]), float(l["qty"])) for l in d["ask"]), key=lambda x: x[0])
    return bids, asks


def book_lighter(pair):
    d = http_json(f"https://mainnet.zklighter.elliot.ai/api/v1/orderBookOrders?market_id={LIGHTER_IDS[pair]}&limit=250")
    bids = sorted(((float(o["price"]), float(o["remaining_base_amount"])) for o in d.get("bids", [])), key=lambda x: -x[0])
    asks = sorted(((float(o["price"]), float(o["remaining_base_amount"])) for o in d.get("asks", [])), key=lambda x: x[0])
    return bids, asks


def _hl_book(coin):
    # l2Book caps at 20 levels/side (~3bp at min ticks). Merge the full-precision
    # book with a coarse nSigFigs=4 book (~31bp range) for depth beyond it.
    def levels(body):
        d = http_json("https://api.hyperliquid.xyz/info", body=body)
        return ([(float(l["px"]), float(l["sz"])) for l in d["levels"][0]],
                [(float(l["px"]), float(l["sz"])) for l in d["levels"][1]])

    fine_bids, fine_asks = levels({"type": "l2Book", "coin": coin})
    coarse_bids, coarse_asks = levels({"type": "l2Book", "coin": coin, "nSigFigs": 4})
    min_fine_bid = fine_bids[-1][0] if fine_bids else 0
    max_fine_ask = fine_asks[-1][0] if fine_asks else float("inf")
    bids = fine_bids + [(p, q) for p, q in coarse_bids if p < min_fine_bid]
    asks = fine_asks + [(p, q) for p, q in coarse_asks if p > max_fine_ask]
    return bids, asks


def book_tradexyz(pair):
    # trade.xyz = HIP-3 builder dex "xyz" on Hyperliquid; same l2Book API,
    # coins prefixed "xyz:". Commodities + equities; specials map below.
    sym = {"XAU": "xyz:GOLD", "XAG": "xyz:SILVER"}.get(pair, f"xyz:{pair}")
    return _hl_book(sym)


def book_qfex(pair):
    # QFEX has no REST book; grab one pulsed level2 snapshot over websocket.
    sym = {"XAU": "GOLD-USD", "XAG": "SILVER-USD"}.get(pair, f"{pair}-USD")
    from websockets.sync.client import connect
    with connect("wss://mds.qfex.com", open_timeout=8) as ws:
        ws.send(json.dumps({"type": "subscribe", "channels": ["level2"], "symbols": [sym]}))
        for _ in range(10):
            msg = json.loads(ws.recv(timeout=8))
            if msg.get("type") == "level2" and msg.get("symbol") == sym:
                bids = sorted(((float(p), float(q)) for p, q in msg["bid"]), key=lambda x: -x[0])
                asks = sorted(((float(p), float(q)) for p, q in msg["ask"]), key=lambda x: x[0])
                return bids, asks
    raise RuntimeError("no level2 snapshot received")


def book_ondo(pair):
    # Ondo Perps (Ondo Finance RWA perp DEX). Symbols like BTC-USD.P; no auth for market data.
    d = http_json(f"https://api.ondoperps.xyz/v1/perps/depth?market={pair}-USD.P&depth=100")["result"]
    bids = sorted(((float(p), float(q)) for p, q in d["bids"]), key=lambda x: -x[0])
    asks = sorted(((float(p), float(q)) for p, q in d["asks"]), key=lambda x: x[0])
    return bids, asks


TXFLOW_IDS = {"XAU": 51, "XAG": 52}


def book_txflow(pair):
    # TXFLOW L1 -- HyperLiquid-fork API discovered from the app bundle (docs say "coming soon").
    # POST /info {type:"l2Book", coin:"<numeric id as string>"}; HL levels format [bids, asks].
    d = http_json("https://api.txflow.com/info",
                  body={"type": "l2Book", "coin": str(TXFLOW_IDS[pair])})
    bids = sorted(((float(l["px"]), float(l["sz"])) for l in d["levels"][0]), key=lambda x: -x[0])
    asks = sorted(((float(l["px"]), float(l["sz"])) for l in d["levels"][1]), key=lambda x: x[0])
    return bids, asks


def book_binance(pair):
    # Binance USDⓈ-M perpetual futures. Public market data, no auth.
    # Note: geo-blocked (HTTP 451) from US IPs -- surfaces as a per-venue error, not a crash.
    d = http_json(f"https://fapi.binance.com/fapi/v1/depth?symbol={pair}USDT&limit=500")
    bids = sorted(((float(p), float(q)) for p, q in d["bids"]), key=lambda x: -x[0])
    asks = sorted(((float(p), float(q)) for p, q in d["asks"]), key=lambda x: x[0])
    return bids, asks


def book_nado(pair):
    sym = {"XAU": "XAUT"}.get(pair, pair)  # Nado's gold perp is Tether Gold
    d = http_json(f"https://gateway.prod.nado.xyz/v2/orderbook?ticker_id={sym}-PERP_USDT0&depth=100")
    bids = sorted(((float(p), float(q)) for p, q in d["bids"]), key=lambda x: -x[0])
    asks = sorted(((float(p), float(q)) for p, q in d["asks"]), key=lambda x: x[0])
    return bids, asks


def book_pacifica(pair):
    # Book returns max 10 levels per agg level (~2bp at agg=1). Merge fine
    # (agg=1) with coarse (agg=10, ~15bp coverage) beyond the fine range.
    def levels(agg):
        d = http_json(f"https://api.pacifica.fi/api/v1/book?symbol={pair}&agg_level={agg}")["data"]["l"]
        bids = sorted(((float(x["p"]), float(x["a"])) for x in d[0]), key=lambda v: -v[0])
        asks = sorted(((float(x["p"]), float(x["a"])) for x in d[1]), key=lambda v: v[0])
        return bids, asks

    bids, asks = levels(1)
    for agg in (10, 100):  # extend coverage tier by tier (~2bp -> ~15bp -> ~140bp)
        cb, ca = levels(agg)
        min_bid = bids[-1][0] if bids else 0
        max_ask = asks[-1][0] if asks else float("inf")
        bids = bids + [(p, q) for p, q in cb if p < min_bid]
        asks = asks + [(p, q) for p, q in ca if p > max_ask]
    return bids, asks


def book_standx(pair):
    # Docs: level ordering not guaranteed — sort client-side.
    d = http_json(f"https://perps.standx.com/api/query_depth_book?symbol={pair}-USD")
    bids = sorted(((float(p), float(q)) for p, q in d["bids"]), key=lambda x: -x[0])
    asks = sorted(((float(p), float(q)) for p, q in d["asks"]), key=lambda x: x[0])
    return bids, asks


VENUES = {
    "RISEx": book_risex,
    "Extended": book_extended,
    "Lighter": book_lighter,
    "Nado": book_nado,
    "Pacifica": book_pacifica,
    "StandX": book_standx,
    "TradeXYZ": book_tradexyz,
    "QFEX": book_qfex,
    "Ondo": book_ondo,
    "Binance": book_binance,
    "TXFLOW": book_txflow,
}
# Which pairs each venue lists.
_METALS = {"XAU", "XAG"}
# Equities = the two stocks listed on RISEx. Comparison set per user: Extended,
# Ondo, Lighter, TradeXYZ, Nado, QFEX, Binance.
_EQUITIES = {"SNDK", "SPCX"}
VENUE_PAIRS = {
    "RISEx": _METALS | _EQUITIES,
    "Extended": _METALS | {"SNDK"},  # SPCX market is REDUCE_ONLY (being delisted), excluded
    "Lighter": _METALS | _EQUITIES,
    "Nado": _METALS | _EQUITIES,  # XAU via XAUT
    "Pacifica": _METALS,
    "StandX": _METALS,
    "TradeXYZ": _METALS | _EQUITIES,
    "QFEX": _METALS | _EQUITIES,     # GOLD-USD / SILVER-USD + {SYM}-USD equities
    "Ondo": _METALS | _EQUITIES,
    # Binance: XAU/XAG + equities trade as TRADIFI_PERPETUAL {SYM}USDT contracts.
    "Binance": _METALS | _EQUITIES,
    "TXFLOW": _METALS,
}


def venue_supports(name, pair):
    return pair in VENUE_PAIRS.get(name, set())

# history[(pair, venue)] = deque of (ts, {notional: (buy_bps, sell_bps)})
_history: dict = {(p, v): deque() for p in PAIRS for v in VENUES}
_history_lock = threading.Lock()


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


def venue_impact(fetch, pair) -> dict:
    bids, asks = fetch(pair)
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
            "buy_impact": buy_px,
            "sell_impact": sell_px,
            "buy_bps": (buy_px - mid) / mid * 1e4 if buy_px else None,
            "sell_bps": (mid - sell_px) / mid * 1e4 if sell_px else None,
        })
    return {"mid": mid, "spread_bps": (best_ask - best_bid) / mid * 1e4, "sims": sims}


def fetch_pair(pair) -> dict:
    from concurrent.futures import ThreadPoolExecutor

    def one(item):
        name, fn = item
        if not venue_supports(name, pair):
            return name, {"error": "not listed on this venue"}
        try:
            return name, venue_impact(fn, pair)
        except Exception as e:
            return name, {"error": f"{type(e).__name__}: {e}"}

    out = {}
    with ThreadPoolExecutor(max_workers=len(VENUES)) as pool:
        for name, d in pool.map(one, VENUES.items()):
            out[name] = d
    return out


def _init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS samples (
        ts INTEGER NOT NULL, pair TEXT NOT NULL, venue TEXT NOT NULL,
        spread_bps REAL, data TEXT NOT NULL)""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_samples ON samples (pair, venue, ts)")
    con.commit()
    con.close()


def _load_history():
    """Rehydrate the in-memory chart window from sqlite so restarts don't wipe it."""
    cutoff = int(time.time() - HISTORY_MIN * 60)
    con = sqlite3.connect(DB_PATH)
    try:
        rows = con.execute(
            "SELECT ts, pair, venue, data FROM samples WHERE ts >= ? ORDER BY ts", (cutoff,)).fetchall()
    finally:
        con.close()
    for ts, pair, venue, data in rows:
        key = (pair, venue)
        if key in _history:
            point = {int(k): tuple(v) for k, v in json.loads(data).items()}
            _history[key].append((ts, point))


def _sampler():
    from concurrent.futures import ThreadPoolExecutor

    def one(job):
        pair, name, fn = job
        try:
            return pair, name, venue_impact(fn, pair)
        except Exception:
            return pair, name, None

    con = sqlite3.connect(DB_PATH)
    jobs = [(p, n, f) for p in PAIRS for n, f in VENUES.items() if venue_supports(n, p)]
    pool = ThreadPoolExecutor(max_workers=len(jobs))  # fully parallel cycle
    while True:
        now = time.time()
        batch = []
        if True:
            for pair, name, d in pool.map(one, jobs):
                if d and "sims" in d:
                    point = {s["notional"]: (s["buy_bps"], s["sell_bps"]) for s in d["sims"]}
                    with _history_lock:
                        _history[(pair, name)].append((now, point))
                    batch.append((int(now), pair, name, d.get("spread_bps"),
                                  json.dumps({str(k): v for k, v in point.items()})))
        if batch:
            con.executemany("INSERT INTO samples VALUES (?,?,?,?,?)", batch)
            con.commit()
        cutoff = now - HISTORY_MIN * 60
        with _history_lock:
            for q in _history.values():
                while q and q[0][0] < cutoff:
                    q.popleft()
        # target a fixed cadence: subtract however long this cycle took
        time.sleep(max(0.5, SAMPLE_SECONDS - (time.time() - now)))


def window_stats(pair, name) -> dict:
    cutoff = time.time() - STATS_WINDOW_MIN * 60
    with _history_lock:
        samples = [(t, pt) for t, pt in _history[(pair, name)] if t >= cutoff]
    out = {"n": len(samples), "sims": {}}
    for n in NOTIONALS:
        buys = [pt[n][0] for _, pt in samples if n in pt and pt[n][0] is not None]
        sells = [pt[n][1] for _, pt in samples if n in pt and pt[n][1] is not None]
        if buys or sells:
            out["sims"][str(n)] = {
                "buy_mean": statistics.fmean(buys) if buys else None,
                "buy_median": statistics.median(buys) if buys else None,
                "sell_mean": statistics.fmean(sells) if sells else None,
                "sell_median": statistics.median(sells) if sells else None,
            }
    return out


def history_series(pair) -> dict:
    out = {}
    with _history_lock:
        for name in VENUES:
            q = _history[(pair, name)]
            entry = {"t": [round(t) for t, _ in q]}
            for n in NOTIONALS:
                entry[str(n)] = {
                    "buy": [pt.get(n, (None, None))[0] for _, pt in q],
                    "sell": [pt.get(n, (None, None))[1] for _, pt in q],
                }
            out[name] = entry
    return out


def _pctl(sorted_vals, q):
    if not sorted_vals:
        return None
    idx = q * (len(sorted_vals) - 1)
    lo, hi = int(idx), min(int(idx) + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def percentiles(pair: str, venue: str, minutes: int) -> dict:
    cutoff = int(time.time() - minutes * 60)
    con = sqlite3.connect(DB_PATH)
    try:
        rows = con.execute(
            "SELECT spread_bps, data FROM samples WHERE pair=? AND venue=? AND ts>=?",
            (pair, venue, cutoff)).fetchall()
    finally:
        con.close()
    spreads = sorted(r[0] for r in rows if r[0] is not None)
    points = [json.loads(r[1]) for r in rows]
    out = {"n": len(rows), "minutes": minutes,
           "spread": {q: _pctl(spreads, p) for q, p in
                      (("p25", .25), ("p50", .5), ("p75", .75), ("p99", .99))},
           "sizes": []}
    for n in NOTIONALS:
        k = str(n)
        buys = sorted(pt[k][0] for pt in points if k in pt and pt[k][0] is not None)
        sells = sorted(pt[k][1] for pt in points if k in pt and pt[k][1] is not None)
        total = sum(1 for pt in points if k in pt)
        out["sizes"].append({
            "notional": n,
            "buy": {q: _pctl(buys, p) for q, p in (("p25", .25), ("p50", .5), ("p75", .75), ("p99", .99))},
            "sell": {q: _pctl(sells, p) for q, p in (("p25", .25), ("p50", .5), ("p75", .75), ("p99", .99))},
            "fill_rate": (min(len(buys), len(sells)) / total * 100) if total else None,
        })
    return out


PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><title>Perp Slippage Compare</title>
<style>
.viz-root, body { /* palette roles (reference palette; light + dark selected) */
  --surface-1:#fcfcfb; --text-primary:#0b0b0b; --text-secondary:#52514e; --line:#e3e4ee;
  --card:#ffffff; --hl:#eef;
  --s-risex:#2a78d6; --s-extended:#1baf7a; --s-lighter:#eda100; --s-hyperliquid:#008300; --s-nado:#4a3aa7;
  --s-01:#e34948; --s-decibel:#e87ba4; --s-hotstuff:#eb6834;
  --s-grvt:#0e9db1; --s-pacifica:#64748b; --s-standx:#8a5a44; --s-perpl:#c026d3; --s-tradexyz:#0369a1; --s-qfex:#4d7c0f; --s-ondo:#b45309; --s-binance:#0f172a; --s-txflow:#0d9488;
  --buy:#0e7a4f; --sell:#b3372f;
}
@media (prefers-color-scheme: dark) {
  .viz-root, body {
    --surface-1:#1a1a19; --text-primary:#ffffff; --text-secondary:#c3c2b7; --line:#2a2a3a;
    --card:#1e1e28; --hl:#22223a;
    --s-risex:#3987e5; --s-extended:#199e70; --s-lighter:#c98500; --s-hyperliquid:#008300; --s-nado:#9085e9;
    --s-01:#e66767; --s-decibel:#d55181; --s-hotstuff:#d95926;
    --s-grvt:#2fb3c6; --s-pacifica:#94a3b8; --s-standx:#b07a5e; --s-perpl:#d946ef; --s-tradexyz:#38bdf8; --s-qfex:#a3e635; --s-ondo:#f59e0b; --s-binance:#e2e8f0; --s-txflow:#2dd4bf;
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
.tabs { display:flex; gap:8px; margin-bottom:16px; align-items:center }
.tabs button { padding:8px 18px; font-size:14px; border:1px solid var(--line); border-radius:8px;
  background:var(--card); color:var(--text-primary); cursor:pointer }
.tabs button.on { background:var(--hl); font-weight:600 }
.card { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:16px 18px; margin-bottom:16px }
.card h2 { font-size:15px; margin-bottom:10px }
table { width:100%; border-collapse:collapse; font-variant-numeric:tabular-nums }
th { color:var(--text-secondary); font-size:11px; text-transform:uppercase; letter-spacing:.04em; text-align:right; padding:4px 8px }
th:first-child { text-align:left }
td { padding:7px 8px; text-align:right; border-top:1px solid var(--line) }
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
<h1>Perp slippage — venue comparison</h1>
<div class="sub">simulated market orders ($10k / $25k / $50k / $100k per side) &middot; impact price = fill VWAP walking the live book &middot;
slippage = distance from the venue's mid, in bps &middot; sampled every 10s server-side, 60 min retained</div>
<div class="tabs">
  <button id="tab-live" class="on">Live</button>
  <button id="tab-time">Over time</button>
  <button id="tab-perc">Percentiles</button>
  <button id="tab-fees">Fee schedules</button>
  <span style="width:12px"></span>
  <button id="fee-toggle" title="add each venue's base taker fee to the slippage numbers">Fees: off</button>
</div>
<div class="tabs" id="selP" style="margin-top:-8px">__PAIR_SELECTOR__</div>
<div class="tabs" id="venueFilter" style="margin-top:-8px;gap:6px"></div>

<div id="view-live">
  <div class="controls">
    <button class="step" id="dec" title="refresh more often">&minus;</button>
    <div class="val"><span id="ival">500ms</span> refresh</div>
    <button class="step" id="inc">+</button>
    <button id="pause">Pause</button>
    <span id="status" class="sub" style="margin:0"></span>
  </div>
  <div class="card"><h2>Live mid &amp; spread</h2><div id="summary"></div></div>
  <div id="sims"></div>
</div>

<div id="view-time" style="display:none">
  <div class="controls">
    <span class="mut">size</span>
    <span class="seg" id="selN">
      <button data-n="10000" class="on">$10k</button><button data-n="25000">$25k</button><button data-n="50000">$50k</button><button data-n="100000">$100k</button><button data-n="250000">$250k</button><button data-n="500000">$500k</button>
    </span>
    <span class="mut">window</span>
    <span class="seg" id="selW">
      <button data-w="10" class="on">10m</button><button data-w="30">30m</button><button data-w="60">60m</button>
    </span>
    <span class="mut">smoothing</span>
    <span class="seg" id="selS">
      <button data-s="raw">Raw</button><button data-s="ema">EMA 3m</button><button data-s="med" class="on">Median 5m</button>
    </span>
  </div>
  <div class="card"><h2>Buy side — slippage from mid (bps, log scale)</h2>
    <div class="legend" id="legend-buy"></div>
    <div class="chartwrap"><svg id="chart-buy" width="100%" height="260"></svg><div class="tooltip" id="tip-buy"></div></div></div>
  <div class="card"><h2>Sell side — slippage from mid (bps, log scale)</h2>
    <div class="legend" id="legend-sell"></div>
    <div class="chartwrap"><svg id="chart-sell" width="100%" height="260"></svg><div class="tooltip" id="tip-sell"></div></div></div>
</div>
<div id="view-perc" style="display:none">
  <div class="controls">
    <span class="mut">venue</span>
    <select id="selV" style="font-size:14px;padding:6px 10px;border:1px solid var(--line);border-radius:8px;background:var(--card);color:var(--text-primary)"></select>
    <span class="mut">timeframe</span>
    <span class="seg" id="selT">
      <button data-m="10">10m</button><button data-m="30">30m</button><button data-m="60" class="on">1h</button>
      <button data-m="180">3h</button><button data-m="720">12h</button><button data-m="1440">24h</button><button data-m="4320">3d</button><button data-m="10080">7d</button>
    </span>
  </div>
  <div class="card"><h2 id="perc-title">Slippage percentiles</h2><div id="perc-table"></div>
    <div class="mut" style="font-size:12px;margin-top:8px">bps from mid; lower is better. fill% = share of samples where the book could fill the size on both sides.
    Data persists to disk from when the collector first ran, so long timeframes fill in over time.</div></div>
</div>
<div id="view-fees" style="display:none">
  <div class="card"><h2>Taker fee schedules — base tier, no discounts</h2><div id="fees-table"></div>
    <div class="mut" style="font-size:12px;margin-top:8px">Worst-case (entry-tier) taker fees from official venue docs, verified 2026-07-19/21.
    These are the numbers the "Fees: on" toggle adds to slippage. Volume tiers, staking and token discounts all lower them &mdash;
    the base tier is used so every venue is compared at the same starting line.</div></div>
</div>
<div class="stamp" id="stamp"></div>

<script>
let intervalSec = 0.5, timer = null, paused = false, inflight = false;
let curTab = "live", curPair = "XAU", curN = 10000, curW = 10, curS = "med", lastHistory = null;
let curVenue = "RISEx", curT = 60;
const NOTIONALS = [10000, 25000, 50000, 100000, 250000, 500000];
const VENUES = __VENUE_LIST__;
const VENUE_PAIRS = __VENUE_PAIRS__;  // {venue: [pairs it lists]}
const COLORS = { RISEx:"var(--s-risex)", Extended:"var(--s-extended)", Lighter:"var(--s-lighter)",
                 Nado:"var(--s-nado)", Pacifica:"var(--s-pacifica)", StandX:"var(--s-standx)",
                 TradeXYZ:"var(--s-tradexyz)", QFEX:"var(--s-qfex)", Ondo:"var(--s-ondo)",
                 Binance:"var(--s-binance)", TXFLOW:"var(--s-txflow)" };
// Per-market venue filter: hiddenByPair[pair] = [venues switched off for that pair].
// Persisted in localStorage; the chip row below the pair selector and the chart
// legends both toggle the same state.
let hiddenByPair = {};
try { hiddenByPair = JSON.parse(localStorage.getItem("venueFilter") || "{}"); } catch (e) {}
const isHidden = v => (hiddenByPair[curPair] || []).includes(v);
function toggleVenue(v) {
  const arr = hiddenByPair[curPair] || (hiddenByPair[curPair] = []);
  const i = arr.indexOf(v);
  i >= 0 ? arr.splice(i, 1) : arr.push(v);
  try { localStorage.setItem("venueFilter", JSON.stringify(hiddenByPair)); } catch (e) {}
  renderVenueFilter();
  tick();
  if (lastHistory) renderTime();
}
function renderVenueFilter() {
  const el = document.getElementById("venueFilter");
  const chips = VENUES.filter(v => (VENUE_PAIRS[v] || []).includes(curPair)).map(v =>
    `<button data-v="${v}" style="padding:4px 10px;font-size:12px;${isHidden(v) ? "opacity:.35" : ""}" ` +
    `title="${isHidden(v) ? "click to show" : "click to hide"} ${v} on ${curPair}">` +
    `<span class="chip" style="background:${COLORS[v]}"></span> ${v}</button>`);
  el.innerHTML = `<span class="mut" style="font-size:12px">venues</span>` + chips.join("");
  for (const b of el.querySelectorAll("button")) b.onclick = () => toggleVenue(b.dataset.v);
}
// Base-tier (worst) taker fees, bps, from official docs 2026-07-19.
// TradeXYZ = HIP-3 GROWTH mode (standard mode would be 9.0).
const TAKER_FEE_BPS = { RISEx:3.0, Extended:2.5, Lighter:0, Nado:3.5,
  Pacifica:4.0, StandX:4.0, TradeXYZ:0.9, QFEX:5.0, Ondo:3.5,
  Binance:5.0, TXFLOW:4.5 };  // Binance USDS-M VIP 0 (no BNB discount); TXFLOW VIP 0
// Schedule context for the Fee-schedules tab (tier used + how the schedule works).
const FEE_NOTES = {
  Lighter:     ["Standard accounts", "0 maker / 0 taker &mdash; docs say &quot;currently&quot;, permanence not stated. Opt-in Premium accounts pay 2.8bp taker."],
  TradeXYZ:    ["HIP-3 Growth Mode", "&ge;90% off for Growth-flagged assets (GOLD/SILVER confirmed). Standard mode would be 9.0bp: 4.5 HL base + 4.5 builder. Equities assumed Growth Mode too &mdash; unverified."],
  Extended:    ["Flat", "One flat schedule for everyone: 0 maker / 2.5 taker."],
  RISEx:       ["Base schedule", "1bp maker / 3bp taker, from the mainnet fee config. MM accounts have negotiated overrides."],
  Nado:        ["Entry tier ($0 30d volume)", "Volume-tiered; discounts start as 30-day volume grows."],
  Ondo:        ["Base tier", "RWA/tokenized-equity perp DEX; same fee across its markets."],
  Pacifica:    ["Tier 1", "Volume-tiered schedule."],
  StandX:      ["Flat", "1bp maker / 4bp taker, appears untiered."],
  QFEX:        ["Commodities class", "Fees vary by asset class: FX 2bp, commodities/indices 5bp, single stocks 10bp. 5bp shown; the toggle automatically applies 10bp on SNDK/SPCX."],
  Binance:     ["VIP 0, USD&#x24C8;-M futures", "Tiered to 1.7bp taker at VIP 9; paying fees in BNB takes a further 10% off. Metals and equities trade as TRADIFI perpetuals, same schedule assumed."],
  TXFLOW:      ["VIP 0 (<$5M 14d volume)", "7-tier schedule down to 2.4bp taker at VIP 6 ($2B+); one flat schedule across all asset classes. HyperLiquid-fork L1."],
};
function renderFees() {
  const rows = VENUES.slice().sort((a, b) => (TAKER_FEE_BPS[a] ?? 0) - (TAKER_FEE_BPS[b] ?? 0)).map(v => {
    const fee = TAKER_FEE_BPS[v] ?? 0;
    const [tier, note] = FEE_NOTES[v] || ["", ""];
    return `<tr><td style="text-align:left"><span class="chip" style="background:${COLORS[v]}"></span> ${v}</td>` +
      `<td><b>${fee.toFixed(1)}</b></td><td class="mut">${(fee / 100).toFixed(3)}%</td>` +
      `<td style="text-align:left" class="mut">${tier}</td>` +
      `<td style="text-align:left;max-width:520px" class="mut">${note}</td></tr>`;
  }).join("");
  document.getElementById("fees-table").innerHTML =
    `<table><thead><tr><th style="text-align:left">venue</th><th>taker (bps)</th><th>taker (%)</th>` +
    `<th style="text-align:left">tier shown</th><th style="text-align:left">schedule notes</th></tr></thead><tbody>${rows}</tbody></table>`;
}
let feesOn = false;
// Per-pair fee exceptions: QFEX prices by asset class (single stocks 10bp vs 5bp base).
const FEE_PAIR_OVERRIDES = { QFEX: { SNDK: 10.0, SPCX: 10.0 } };
const feeFor = venue => (FEE_PAIR_OVERRIDES[venue] || {})[curPair] ?? TAKER_FEE_BPS[venue] ?? 0;
const adj = (v, venue) => (v === null || v === undefined) ? v : v + (feesOn ? feeFor(venue) : 0);
const fmtMid = v => v.toLocaleString(undefined, {maximumSignificantDigits: 7});
const fmtBps = v => (v === null || v === undefined) ? "n/a" : v.toFixed(2);
const fmtN = n => "$" + (n/1000) + "k";

/* ---------- Live tab ---------- */
function summaryTable(data) {
  const rows = Object.entries(data).filter(([name, d]) => !isHidden(name) && d.error !== "not listed on this venue").map(([name, d]) => {
    if (d.error) return `<tr><td>${name}</td><td colspan=3 class="err">${d.error}</td></tr>`;
    return `<tr><td><span class="chip" style="background:${COLORS[name]}"></span> ${name}</td><td>${fmtMid(d.mid)}</td><td>${d.spread_bps.toFixed(3)} bp</td>
      <td class="mut">${d.stats.n} samples</td></tr>`;
  }).join("");
  return `<table><tr><th>venue</th><th>mid</th><th>spread</th><th>10m window</th></tr>${rows}</table>`;
}

function simTable(data, notional) {
  let bestBuy = Infinity, bestSell = Infinity;
  for (const [name, d] of Object.entries(data)) {
    if (isHidden(name)) continue;
    const s = (d.sims || []).find(x => x.notional === notional);
    if (s) {
      const b = adj(s.buy_bps, name), a = adj(s.sell_bps, name);
      if (b !== null) bestBuy = Math.min(bestBuy, b);
      if (a !== null) bestSell = Math.min(bestSell, a);
    }
  }
  const rows = Object.entries(data).filter(([name, d]) => !isHidden(name) && d.error !== "not listed on this venue").map(([name, d]) => {
    if (d.error) return `<tr><td>${name}</td><td colspan=6 class="err">unavailable</td></tr>`;
    const s = (d.sims || []).find(x => x.notional === notional);
    const st = (d.stats.sims || {})[String(notional)] || null;
    if (!s) return "";
    const b = adj(s.buy_bps, name), a = adj(s.sell_bps, name);
    return `<tr><td><span class="chip" style="background:${COLORS[name]}"></span> ${name}</td>
      <td class="buyv ${b===bestBuy?'best':''}">${fmtBps(b)}</td>
      <td class="sellv ${a===bestSell?'best':''}">${fmtBps(a)}</td>
      <td class="buyv mut">${st?fmtBps(adj(st.buy_mean, name)):"-"}</td><td class="buyv mut">${st?fmtBps(adj(st.buy_median, name)):"-"}</td>
      <td class="sellv mut">${st?fmtBps(adj(st.sell_mean, name)):"-"}</td><td class="sellv mut">${st?fmtBps(adj(st.sell_median, name)):"-"}</td></tr>`;
  }).join("");
  const label = feesOn ? "all-in taker cost (slippage + base fee, bps)" : "slippage from mid (bps)";
  return `<div class="card"><h2>${fmtN(notional)} market order &mdash; ${label}</h2><table>
    <tr><th>venue</th><th>buy live</th><th>sell live</th><th>buy mean 10m</th><th>buy med 10m</th><th>sell mean 10m</th><th>sell med 10m</th></tr>
    ${rows}</table></div>`;
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
  for (const v of VENUES) {
    if (isHidden(v)) continue;
    const h = hist[v]; if (!h) continue;
    const pts = [];
    for (let i = 0; i < h.t.length; i++) {
      if (h.t[i] < t0) continue;
      pts.push([h.t[i], adj(h[String(curN)][side][i], v)]);
    }
    rawSeries[v] = pts;
    series[v] = smoothSeries(pts, curS);
    for (const [, y] of series[v]) if (y !== null && y > 0) vals.push(y);
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
    for (const v of VENUES) {
      const dstr = pathFor(rawSeries[v] || []);
      if (dstr) g += `<path d="${dstr}" fill="none" stroke="${COLORS[v]}" stroke-width="1" opacity="0.22"/>`;
    }
  }
  for (const v of VENUES) {
    const dstr = pathFor(series[v] || []);
    if (dstr) g += `<path d="${dstr}" fill="none" stroke="${COLORS[v]}" stroke-width="2" stroke-linejoin="round"/>`;
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
    for (const v of VENUES) {
      const pts = series[v] || [];
      if (!pts.length) continue;
      let best = null;
      for (const p of pts) if (!best || Math.abs(p[0]-t) < Math.abs(best[0]-t)) best = p;
      if (!best || Math.abs(best[0]-t) > 30) continue;
      xline = best[0];
      entries.push([v, best[1]]);
    }
    if (!entries.length) { tip.style.display = "none"; return; }
    // rank best -> worst (lowest slippage first, n/a last)
    entries.sort((a, b) => (a[1] === null) - (b[1] === null) || a[1] - b[1]);
    const rows = entries.map(([v, val], i) =>
      `<div class="trow"><span><span class="mut">${i+1}.</span> <span class="chip" style="background:${COLORS[v]}"></span>${v}</span><b>${fmtBps(val)}</b></div>`).join("");
    const xl = document.getElementById(svgId + "-x");
    if (xl && xline) { xl.setAttribute("x1", X(xline)); xl.setAttribute("x2", X(xline)); }
    tip.innerHTML = `<div class="mut" style="margin-bottom:4px">${new Date((xline||t)*1000).toLocaleTimeString()} &middot; ${curPair} ${fmtN(curN)} ${side}</div>` + rows;
    tip.style.display = "block";
    tip.style.left = Math.min(mx + 14, W - 180) + "px";
    tip.style.top = "14px";
  };
  svg.onmouseleave = () => { tip.style.display = "none"; };

  // Legend ranked best -> worst by latest smoothed slippage for THIS side.
  const legendId = svgId === "chart-buy" ? "legend-buy" : "legend-sell";
  const ranked = [];
  for (const v of VENUES) {
    if (isHidden(v)) continue;
    const pts = series[v] || [];
    let last = null;
    for (let i = pts.length - 1; i >= 0; i--)
      if (pts[i][1] !== null && pts[i][1] > 0) { last = pts[i][1]; break; }
    ranked.push([v, last]);
  }
  ranked.sort((a, b) => (a[1] === null) - (b[1] === null) || a[1] - b[1]);
  const items = ranked.map(([v, val], i) =>
    `<span class="legitem" data-v="${v}" style="cursor:pointer" title="click to hide">` +
    `<span class="mut">${i+1}.</span> <span class="chip" style="background:${COLORS[v]}"></span>${v}` +
    ` <b>${val === null ? "n/a" : val.toFixed(2)}</b></span>`);
  for (const v of VENUES) if (isHidden(v))
    items.push(`<span class="legitem" data-v="${v}" style="cursor:pointer;opacity:.35" title="click to show">` +
      `<span class="chip" style="background:${COLORS[v]}"></span>${v}</span>`);
  document.getElementById(legendId).innerHTML = items.join("");
  for (const el of document.querySelectorAll(`#${legendId} .legitem`)) {
    el.onclick = () => {
      const v = el.dataset.v;
      toggleVenue(v);
    };
  }
}

function renderTime() {
  if (!lastHistory) return;
  drawChart("chart-buy", "tip-buy", lastHistory, "buy");
  drawChart("chart-sell", "tip-sell", lastHistory, "sell");
}

/* ---------- Percentiles tab ---------- */
function renderPerc(d) {
  document.getElementById("perc-title").textContent =
    `${curVenue} ${curPair} — slippage percentiles, last ${curT >= 60 ? (curT/60)+"h" : curT+"m"} (${d.n} samples)`;
  const P = ["p25","p50","p75","p99"];
  const f = v => (v === null || v === undefined) ? "n/a" : v.toFixed(2);
  let html = `<table><tr><th>size</th>${P.map(p=>`<th>buy ${p}</th>`).join("")}${P.map(p=>`<th>sell ${p}</th>`).join("")}<th>fill%</th></tr>`;
  html += `<tr><td>spread</td>${P.map(p=>`<td class="mut">${f(d.spread[p])}</td>`).join("")}<td colspan=5 class="mut">quoted spread, bps</td></tr>`;
  for (const s of d.sizes) {
    html += `<tr><td>${fmtN(s.notional)}</td>` +
      P.map(p=>`<td class="buyv">${f(adj(s.buy[p], curVenue))}</td>`).join("") +
      P.map(p=>`<td class="sellv">${f(adj(s.sell[p], curVenue))}</td>`).join("") +
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
      const r = await fetch("/depth?pair=" + curPair);
      const data = await r.json();
      document.getElementById("summary").innerHTML = summaryTable(data);
      document.getElementById("sims").innerHTML = NOTIONALS.map(n => simTable(data, n)).join("");
    } else if (curTab === "time") {
      const r = await fetch("/history?pair=" + curPair);
      lastHistory = await r.json();
      renderTime();
    } else if (curTab === "fees") {
      renderFees();  // static -- no fetch needed
      document.getElementById("status").textContent = "";
      inflight = false;
      return;
    } else {
      const r = await fetch(`/percentiles?pair=${curPair}&venue=${encodeURIComponent(curVenue)}&minutes=${curT}`);
      renderPerc(await r.json());
    }
    document.getElementById("stamp").textContent = "last update " + new Date().toLocaleTimeString() +
      " - " + curPair + " - lower is better; slippage includes half the spread since it is measured from mid";
    document.getElementById("status").textContent = "";
  } catch (e) {
    document.getElementById("status").textContent = "fetch failed: " + e;
  } finally { inflight = false; }
}

function arm() { clearInterval(timer); timer = setInterval(tick, intervalSec*1000); }
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
  for (const [id, key] of [["tab-live","live"],["tab-time","time"],["tab-perc","perc"],["tab-fees","fees"]])
    document.getElementById(id).classList.toggle("on", t === key);
  for (const [id, key] of [["view-live","live"],["view-time","time"],["view-perc","perc"],["view-fees","fees"]])
    document.getElementById(id).style.display = t === key ? "" : "none";
  tick();
}
document.getElementById("tab-live").onclick = () => setTab("live");
document.getElementById("tab-time").onclick = () => setTab("time");
document.getElementById("tab-perc").onclick = () => setTab("perc");
document.getElementById("tab-fees").onclick = () => setTab("fees");
document.getElementById("fee-toggle").onclick = e => {
  feesOn = !feesOn;
  e.target.textContent = feesOn ? "Fees: on" : "Fees: off";
  e.target.style.fontWeight = feesOn ? "600" : "";
  tick();          // re-render live / percentiles from fresh data
  renderTime();    // charts re-render instantly from cached history
};

// venue dropdown + timeframe selector for the Percentiles tab
const selV = document.getElementById("selV");
selV.innerHTML = VENUES.map(v => `<option value="${v}" ${v===curVenue?"selected":""}>${v}</option>`).join("");
selV.onchange = () => { curVenue = selV.value; tick(); };
document.getElementById("selT").onclick = e => {
  if (e.target.dataset.m) {
    curT = Number(e.target.dataset.m);
    for (const b of document.querySelectorAll("#selT button")) b.classList.toggle("on", b === e.target);
    tick();
  }
};

document.getElementById("selP").onclick = e => {
  if (e.target.dataset.p) {
    curPair = e.target.dataset.p;
    for (const b of document.querySelectorAll("#selP button")) b.classList.toggle("on", b === e.target);
    renderVenueFilter();
    tick();
  }
};
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
window.addEventListener("resize", renderTime);
renderVenueFilter(); tick(); arm();
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
            self.send_header("WWW-Authenticate", 'Basic realm="venue-screener"')
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
            sel = "".join(
                f'<span class="mut" style="font-size:12px;text-transform:uppercase;letter-spacing:.04em">{cls}</span>'
                + '<span class="seg">'
                + "".join(f'<button data-p="{p}"{" class=\"on\"" if p == PAIRS[0] else ""}>{p}</button>' for p in ps)
                + "</span>"
                for cls, ps in ASSET_CLASSES.items())
            vp = {v: [p for p in PAIRS if venue_supports(v, p)] for v in VENUES}
            body = (PAGE.replace("__VENUE_LIST__", json.dumps(list(VENUES)))
                        .replace("__VENUE_PAIRS__", json.dumps(vp))
                        .replace("__PAIR_SELECTOR__", sel)).encode()
            ctype = "text/html; charset=utf-8"
        elif path == "/depth":
            out = fetch_pair(pair)
            for name in out:
                out[name]["stats"] = window_stats(pair, name)
            body = json.dumps(out).encode()
            ctype = "application/json"
        elif path == "/history":
            body = json.dumps(history_series(pair)).encode()
            ctype = "application/json"
        elif path == "/percentiles":
            venue = (qs.get("venue") or ["RISEx"])[0]
            minutes = int((qs.get("minutes") or ["60"])[0])
            body = json.dumps(percentiles(pair, venue, minutes)).encode()
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
    print(f"Perp slippage comparison -> http://{host}:{port}")
    ThreadingHTTPServer((host, port), Handler).serve_forever()
