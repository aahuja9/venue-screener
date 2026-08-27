# RISEx Slippage Screener

Single-venue depth screener for RISEx: simulates market orders of
$10k / $50k / $100k / $500k / $1M against the live RISEx order book on both
sides and reports slippage from mid in bps.

- Pairs: BTC, ETH, SOL, HYPE (crypto), XAU (commodity), SPY, QQQ, SNDK (equities)
- Four tabs:
  - **Live** — pairs x sizes matrix, buy and sell side, plus a 10-minute median matrix
  - **Liquidity** — USD notional resting within 1 / 2.5 / 5 bp of mid, EMA-smoothed
    (30s time constant, recomputed every 5s), bid / ask / both
  - **Over time** — one line per pair for a chosen size, raw / EMA 3m / median 5m
  - **Percentiles** — P25/50/75/99 per size, over 10m to 7d
- Samples every 5s into `risex_samples.db` (SQLite); history survives restarts
- Market ids are read from `https://api.rise.trade/v1/markets`
- `n/a` means the visible book could not fill that size on that side
- RISEx taker fee is 3bp and is *not* added to the numbers shown

## Run

```
python3 btc_venue_compare.py
# open http://localhost:8900
```

Set `DASH_PASSWORD` to require HTTP Basic auth. Set `PORT` (Render does) to bind
all interfaces.
