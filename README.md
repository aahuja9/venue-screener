# Venue Screener

Cross-venue perp liquidity screener: simulates market orders ($10k-$500k, both sides)
against live order books and reports slippage from mid in bps.

- 14 venues: RISEx, Extended, Lighter, HyperLiquid, Nado, 01, HotStuff, GRVT,
  Pacifica, StandX, Perpl, TradeXYZ (HIP-3), QFEX, Ondo (+ Decibel with APTOS_API_KEY)
- Pairs: BTC, ETH, SOL, HYPE, XAU, XAG (per-venue availability varies)
- Three tabs: Live (with refresh control + fee toggle), Over time (smoothed charts,
  ranked legends), Percentiles (P25/50/75/99 by venue and timeframe)
- Samples every 5s server-side into venue_samples.db (SQLite); history survives restarts
- Fee toggle adds each venue's base taker fee (TradeXYZ = HIP-3 growth mode 0.9bp)

## Run

```
pip install websockets
python3 btc_venue_compare.py
# open http://localhost:8900
```
