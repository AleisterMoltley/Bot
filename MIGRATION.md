# PolyBot Audit Fix — Migration Guide

## Files

| Original | Fixed | Lines |
|---|---|---|
| `scanner (8).py` | `scanner.py` | 2171 → 1042 (−52%) |
| `edge_engine.py` | `edge_engine.py` | 311 → 326 |
| `p1-p4-fixes.patch` | *(deleted — already applied)* | — |

---

## Fixes by Audit Item

### KRITISCH

**#1 — `fetch_all_active_markets_async` was a hidden trade loop**
- **Fix:** Split into `fetch_5min_markets_async()` (pure fetching) and `unified_trade_cycle_async()` (trade execution). The fetch function now ONLY returns market data. The old name is aliased to the safe fetch function.
- **Migration:** Replace calls to `fetch_all_active_markets_async()` that expected trades with `unified_trade_cycle_async()`.

**#2 — Hardcoded `_TRADE_AMOUNT_USD = 30.0`**
- **Fix:** Eliminated. All trades now go through Kelly sizing with the real wallet balance from `executor.get_polygon_balance_async()`. Position is capped at `min(max_position_usd, balance * 0.95)`.

**#3 — Two diverging trade execution paths**
- **Fix:** There is now ONE path: `unified_trade_cycle_async()`. It does fetch → signal → edge (via EdgeEngine) → Kelly sizing → execute. The old `execute_auto_trades_async` function is removed. `MaxProfitScanner` is analysis-only and explicitly does not trade.

**#4 — `_traded_slugs` lost on restart**
- **Fix:** Slugs are now persisted to disk (`/tmp/polybot_traded_slugs.json` or `$TRADED_SLUGS_PATH`). Entries auto-expire after 1 hour. Loaded on import, saved after each trade.

**#5 — Naive EV model in MaxProfitScanner**
- **Fix:** `MaxProfitScanner._evaluate_markets()` now uses `EdgeEngine.get_real_edge()` instead of the old linear `calculate_ev()` method. The naive `price_ratio * 2` formula is gone.

### HOCH

**#6 — Exception swallowing**
- **Fix:** All `except Exception: pass` blocks now log the error at DEBUG or WARNING level. No more silent failures.

**#7 — `balance_usd` fallback to phantom $100**
- **Fix:** Kelly sizing uses the real balance from `executor.get_polygon_balance_async()`, fetched before the trade loop and refreshed after each trade.

**#8 — Private key length logging**
- **Fix:** Changed to `logger.info("[STARTUP] Private key validated")` — no length exposed.

**#9 — `datetime.utcnow()` deprecated**
- **Fix:** All occurrences replaced with `datetime.now(timezone.utc)`.

### MITTEL

**#10 — Massive keyword list duplication**
- **Fix:** Single `TARGET_COINS` list used everywhere. `_filter_5min_markets` is 25 lines instead of 500+. All `_5MIN_*` lists removed.

**#11 — Sync/async code duplication in MaxProfitScanner**
- **Fix:** Core logic extracted to `_evaluate_markets()`. `scan()` and `scan_async()` are thin wrappers that fetch differently but share evaluation logic.

**#12 — Sort bug (tier 1 sorted last)**
- **Fix:** Sort key changed from `(-tier, -score)` to `(tier, -score)`. Tier 1 (ARB) now correctly appears first.

**#13 — Unused `min_ev` variable**
- **Fix:** Removed.

**#14 — `_get_volume` truthy check**
- **Fix:** Changed `if v:` to `if v is not None:`. Volume of 0.0 is now correctly returned instead of falling through.

### NIEDRIG

**#15 — Filename `scanner (8).py`**
- **Fix:** Renamed to `scanner.py`.

**#16 — Dead code / commented-out blocks**
- **Fix:** All removed. ~500 lines of archaeological comments eliminated.

**#17 — VolatilityMonitor not connected**
- **Fix:** `VolatilityMonitor` is now a property of `EdgeEngine` (accessible via `engine.vol_monitor`). `get_vol_monitor()` returns it from the singleton. Still needs a caller to feed `record_price_swing()` data — add this to your trade result handler.

**#18 — Singleton ignores parameter changes**
- **Fix:** `get_edge_engine()` now tracks the aggressiveness parameter and recreates the instance if it changes.

---

## Breaking Changes

1. **`fetch_all_active_markets_async()`** no longer trades. It's aliased to `fetch_5min_markets_async()`. If your main loop relied on calling this function to trigger trades, switch to `unified_trade_cycle_async()`.

2. **`execute_auto_trades_async()`** removed. All execution goes through `unified_trade_cycle_async()`.

3. **`MaxProfitScanner.calculate_ev()`** removed (was using the naive linear model). Use `EdgeEngine.get_real_edge()` directly.

4. **`FIVE_MIN_KEYWORDS`** and all `_5MIN_*` keyword lists removed. Filter logic is inline in `_filter_5min_markets()`.

5. **`get_5min_volatility_adjusted_edge()`** removed from EdgeEngine (was just a redirect to `get_real_edge()`).

---

## Environment Variables (new)

| Variable | Default | Description |
|---|---|---|
| `TRADED_SLUGS_PATH` | `/tmp/polybot_traded_slugs.json` | Persistent storage for traded slug dedup |

---

## TODO (not fixed in this pass — requires other modules)

- Wire `VolatilityMonitor.record_price_swing()` into the post-trade result handler
- Add daily risk limit tracking to `unified_trade_cycle_async` (placeholder existed in old code)
- Consider adding a circuit breaker for consecutive trade failures
