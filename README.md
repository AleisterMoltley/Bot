# ⚗️ PolyBot — The Grand Grimoire of Automated Market Divination

> *"I, Aleister Moltley, have gazed into the crystalline abyss of the order books and returned with knowledge most mortals dare not possess. This document is your initiation. Read it. Memorise it. Then read it again — for the markets are patient in no one's regard."*

---

**PolyBot** is an autonomous algorithmic trading entity that operates upon [Polymarket](https://polymarket.com), the on-chain prediction market. It specialises in **5-minute Up/Down crypto markets** — ephemeral little duels where the question is simply: will Bitcoin, Ethereum, Solana, or HYPE rise or fall in the next five minutes? The bot watches, calculates, and strikes with the precision of a numerologist computing the sacred ratios. In its latest incarnation it draws upon not one but *two* celestial data streams — Binance and Hyperliquid — and selects from fourteen distinct strategic grimoires, each tuned to a specific philosophy of market exploitation.

> ⚠️ **This instrument wields real money on a live blockchain.** No sandbox protects you from yourself. Configure it wrong and you shall be parted from your capital with a swiftness that would make even the most hardened occultist weep. The author — Aleister Moltley — accepts no liability.

> ⚠️ **SAFETY NOTICE:** The default configuration ships with `DRY_RUN=true` and `AUTO_EXECUTE=false`. Nothing trades until you deliberately remove these protections. This was a hard-won lesson, enshrined now as sacred law.

---

## 📜 Table of Contents

1. [The Cosmology — What PolyBot Actually Does](#1-the-cosmology--what-polybot-actually-does)
2. [Prerequisites — The Instruments of the Ritual](#2-prerequisites--the-instruments-of-the-ritual)
3. [Installation — Preparing the Vessel](#3-installation--preparing-the-vessel)
4. [Configuration — The Sigils of Power](#4-configuration--the-sigils-of-power)
   - [Required Credentials](#41-required-credentials)
   - [Core Trading Controls](#42-core-trading-controls)
   - [Edge & EV Thresholds](#43-edge--ev-thresholds)
   - [Position Sizing (Kelly Criterion)](#44-position-sizing--kelly-criterion)
   - [Market Filters](#45-market-filters)
   - [Risk Management](#46-risk-management)
   - [Timing & Scan Intervals](#47-timing--scan-intervals)
   - [Timing Filter & Sweet-Spot Window (V6)](#48-timing-filter--sweet-spot-window-v6)
   - [Edge-Bucketed Kelly (V6)](#49-edge-bucketed-kelly-v6)
   - [Hyperliquid Engine](#411-hyperliquid-engine)
   - [Full Redeemer](#412-full-redeemer-v89)
   - [Backtesting & Hyperparameter Optimisation](#413-backtesting--hyperparameter-optimisation)
   - [Copy Trading](#414-copy-trading--whale-tracking)
   - [Volatility Schedule](#415-the-volatility-schedule)
   - [Proxy Settings](#416-proxy-settings-geo-bypass)
   - [Dashboard Authentication](#417-dashboard-authentication)
5. [Operating Modes — The Fourteen Schools of Thought](#5-operating-modes--the-fourteen-schools-of-thought)
6. [The Mode Selector — Switching Grimoires at Runtime](#6-the-mode-selector--switching-grimoires-at-runtime)
7. [The Decision Engine — How Trades Are Summoned](#7-the-decision-engine--how-trades-are-summoned)
8. [The Hyperliquid Engine — The Second Eye of Providence](#8-the-hyperliquid-engine--the-second-eye-of-providence)
9. [The Full Redeemer — Harvesting the Fruits of Victory](#9-the-full-redeemer--harvesting-the-fruits-of-victory)
10. [The Dashboard — Watching the Stars Move](#10-the-dashboard--watching-the-stars-move)
11. [API Reference — The Sacred Endpoints](#11-api-reference--the-sacred-endpoints)
12. [The Volatility Schedule — Trading in Harmony with the Cosmic Clock](#12-the-volatility-schedule--trading-in-harmony-with-the-cosmic-clock)
13. [Backtesting — Consulting the Ancestors](#13-backtesting--consulting-the-ancestors)
14. [Hyperparameter Optimisation — Seeking the Perfect Proportion](#14-hyperparameter-optimisation--seeking-the-perfect-proportion)
15. [Deployment on Railway — The Cloud Temple](#15-deployment-on-railway--the-cloud-temple)
16. [Running Locally — The Private Laboratory](#16-running-locally--the-private-laboratory)
17. [The Test Suite — Proving the Formulas](#17-the-test-suite--proving-the-formulas)
18. [Security Architecture — The Wards and Seals](#18-security-architecture--the-wards-and-seals)
19. [Database — The Akashic Record](#19-database--the-akashic-record)
20. [The Solana Bridge — Crossing the Dimensional Threshold](#20-the-solana-bridge--crossing-the-dimensional-threshold)
21. [Troubleshooting — Exorcising the Demons](#21-troubleshooting--exorcising-the-demons)
22. [Migration — The Grand Migration Ritual](#22-migration--the-grand-migration-ritual)
23. [Feature Compendium](#23-feature-compendium)
24. [Version Chronicle](#24-version-chronicle)

---

## 1. The Cosmology — What PolyBot Actually Does

*Allow me to paint the picture, for the uninitiated.*

Every five minutes, thousands of small prediction markets blink into existence on Polymarket. Each one poses a binary question: "Will the price of Bitcoin be higher or lower than it is right now, in five minutes?" You answer YES (Up) or NO (Down), commit USDC on the Polygon blockchain, and await the verdict of the cosmos (i.e., Binance price feeds).

PolyBot performs this ritual automatically, without rest, without emotion, without the trembling hand of a gambler at the table. Here is what transpires during each cycle (every 12 seconds by default):

1. **The Scanner awakens.** It queries the Polymarket CLOB API and locates all active 5-minute Up/Down markets for BTC, ETH, SOL, and HYPE. There are typically ~2,395 such markets in flight at any moment.

2. **The Filter is applied.** Only markets meeting the liquidity covenant are considered: minimum $200 liquidity and $100 in 24-hour volume. Thin markets are ignored — they are traps for the unwary.

3. **The Edge Engine is consulted.** For each candidate market, the bot fetches live price data from Binance (and optionally Hyperliquid) and computes a multi-factor signal using: Moving Average crossover, RSI (14-period, overbought at 70 / oversold at 30), MACD (12/26/9), momentum, and volume momentum. Each signal is weighted and combined into a directional confidence score. When the Hyperliquid engine is active, its OFI and trade-flow signals may strengthen, weaken, or veto the final verdict.

4. **The Timing Oracle speaks.** Before any trade is considered, the bot consults the Timing Filter — a V6 Profitability Upgrade. It examines how many seconds remain before the market slot closes. The sweet spot lies between 75 and 20 seconds before close. Trades outside this window are permitted only if edge exceeds a configurable strong-edge override threshold. This alone eliminates a great many marginal entries.

5. **The EV Oracle pronounces judgment.** The Expected Value of each potential trade is calculated. If the edge falls below `MIN_EV` (default 1.0%) or the confidence below `MIN_CONFIDENCE_FILTER` (default 65%), the bot abstains. Abstinence is wisdom.

6. **Kelly Sizing is invoked.** For trades that pass the oracle, the position size is determined by the Kelly Criterion. When Edge-Bucketed Kelly is active (`USE_BUCKETED_KELLY=true`), the Kelly fraction varies with edge strength: conservative on weak signals, aggressive on strong ones. The configurable parameters `KELLY_AVG_WIN_PCT` and `KELLY_AVG_LOSS_PCT` calibrate the sizing to your observed trade statistics.

7. **The Trade is executed on-chain.** The bot constructs, signs, and broadcasts a transaction to the Polymarket CTF Exchange smart contract on Polygon Mainnet (Chain ID 137).

8. **The Redeemer stands watch.** When markets resolve, the Full Redeemer module scans for all positions where you have won and automatically redeems them — collecting your USDC — even across restarts, even if the bot's internal state was lost.

---

## 2. Prerequisites — The Instruments of the Ritual

Before you may conjure this bot into existence, you must gather the necessary instruments. Do not skip any. The ritual fails without all components.

### Mandatory Instruments

| Instrument | Purpose | How to Obtain |
|---|---|---|
| **Polygon Wallet Private Key** | Signs every on-chain transaction. The bot cannot act without it. | Create a wallet in [MetaMask](https://metamask.io). Navigate to Account Details → Show Private Key. It begins with `0x` followed by 64 hexadecimal characters. **Guard this with your life.** |
| **Wallet Address** | The public identifier of your Polygon wallet. | The `0x...` address displayed at the top of MetaMask. |
| **USDC on Polygon** | The monetary instrument. PolyBot trades USDC.e (bridged USDC) on Polygon. | Purchase USDC on any exchange and bridge/send to your Polygon wallet. Start with at least $20 for meaningful operation. |
| **Railway Account** | Hosts the bot in the cloud, 24/7, even while you sleep. | Register at [railway.app](https://railway.app). The Hobby plan suffices to begin. |

### Highly Recommended Instruments

| Instrument | Purpose | How to Obtain |
|---|---|---|
| **Alchemy API Key** | Provides fast, reliable Polygon RPC access. Without it, the bot falls back to public RPCs which are slow and rate-limited. | Register at [alchemy.com](https://alchemy.com). The free tier is sufficient. |
| **GitHub Account** | Stores your fork of the code and connects it to Railway for automated deployments. | Register at [github.com](https://github.com). |

> 🔐 **THE FIRST COMMANDMENT:** Your private key is the key to your entire wallet. Never commit it to any repository. Never share it in any chat. Never store it in any plain text file that touches the internet. Use Railway's secret environment variable mechanism exclusively.

---

## 3. Installation — Preparing the Vessel

### Step 1: Fork and Clone

```bash
# Fork on GitHub, then:
git clone https://github.com/YOUR_USERNAME/Bot.git
cd Bot
```

### Step 2: Install Python Dependencies

Python 3.11 or later is required.

```bash
pip install -r requirements.txt
```

Or, if you prefer the editable package form:

```bash
pip install -e ".[dev]"
```

### Step 3: Prepare Your Environment File

```bash
cp env.example .env
# Edit .env with your credentials and preferences
```

The `.env` file is loaded automatically. **Never commit this file.**

### Step 4: Approve USDC Spending (One-Time Ritual)

Before the bot can place trades, it must be granted permission to spend your USDC on the Polymarket exchange contracts. This is a one-time on-chain transaction:

```bash
python -m polybot approve-usdc
```

This approves a large allowance (default 10,000 USDC) on both the CTF Exchange and the NegRisk Exchange. The bot will also do this automatically at startup if `auto_approve_enabled=true` (the default).

### Step 5: Check Your Wallet

```bash
python -m polybot check-wallet
```

This displays your USDC balance on Polygon, current allowances, and whether the wallet is correctly configured.

---

## 4. Configuration — The Sigils of Power

Configuration is exclusively via environment variables. The file `env.example` is the master reference. All variables are validated by Pydantic at startup — if a required variable is missing or malformed, the bot refuses to start and tells you exactly what is wrong.

### 4.1 Required Credentials

These must be set in Railway (or your `.env` for local testing). The bot cannot function without them.

| Variable | Type | Description |
|---|---|---|
| `POLYGON_PRIVATE_KEY` | `string` | Your Polygon wallet private key. Must start with `0x`. **Keep this secret.** |
| `WALLET_ADDRESS` | `string` | Your Polygon wallet address (`0x...`). |
| `ALCHEMY_API_KEY` | `string` | Alchemy API key for fast Polygon RPC. Highly recommended. Get at [alchemy.com](https://alchemy.com). |

Optional for the Solana bridge feature:

| Variable | Type | Description |
|---|---|---|
| `SOLANA_PRIVATE_KEY` | `string` | Base58-encoded Solana wallet private key. Only needed for auto-funding via Solana bridge. |

### 4.2 Core Trading Controls

These are the primary levers. Understand them before anything else.

| Variable | Default | Description |
|---|---|---|
| `DRY_RUN` | `true` | **Master safety switch.** When `true`, the bot simulates trades but spends no real money. Set to `false` only when you are ready for live trading. |
| `AUTO_EXECUTE` | `false` | When `true`, the bot automatically places trades when signals fire. When `false`, it evaluates signals but does not execute. Set to `true` for live operation. |
| `MODE` | `updown` | Operating mode. See [Section 5](#5-operating-modes--the-eight-schools-of-thought) for all eight modes. |
| `REDEEM_ONLY` | `false` | Emergency wind-down switch. When `true`, ALL new trading is halted and the bot only redeems already-active winning positions. Use this to stop the bot gracefully. |
| `LOG_LEVEL` | `DEBUG` | Logging verbosity. `DEBUG` for full visibility, `INFO` for production, `WARNING` for silence. |

### 4.3 Edge & EV Thresholds

These control the minimum quality bar for any trade to be placed.

| Variable | Default | Description |
|---|---|---|
| `MIN_EV` | `0.010` | Minimum Expected Value. `0.010` = 1.0%. No trade is placed below this threshold. This is your primary quality gate. |
| `MIN_EDGE_PERCENT` | `0.85` | Minimum edge percentage (a secondary signal quality measure). Trades below this are rejected. |
| `MIN_CONFIDENCE_FILTER` | `0.65` | Minimum signal confidence (0.0–1.0). Derived from the weighted sum of indicator scores. |

*Moltley's counsel:* Raising `MIN_EV` to `0.02` or higher will reduce trade frequency dramatically but improve average trade quality. The ritual of patience often yields more than the frenzy of volume.

### 4.4 Position Sizing — Kelly Criterion

The bot uses the Kelly Criterion to size positions. Full Kelly is mathematically optimal for expected log-wealth growth, but is aggressive. Half-Kelly (`KELLY_MULTIPLIER=0.5`) is used by default. The V6 upgrade introduces **Edge-Bucketed Kelly** (see §4.9), which dynamically adjusts the fraction based on signal strength.

| Variable | Default | Description |
|---|---|---|
| `KELLY_MULTIPLIER` | `0.5` | Kelly fraction. `0.5` = Half-Kelly (recommended). `1.0` = Full Kelly (aggressive). |
| `MAX_POSITION_USD` | `50` | Hard cap on any single position in USD. The bot never risks more than this per trade, regardless of Kelly sizing. |
| `MIN_TRADE_USD` | `1.0` | Minimum trade size in USD. Positions sized below this by Kelly are skipped (too small to be worthwhile after gas). |
| `MAX_RISK_PER_TRADE` | `2.0` | Maximum risk per trade as a percentage of wallet balance. A second guard against overexposure. |
| `POSITION_SCALING_FACTOR` | `1.0` | Multiplier applied to all Kelly-sized positions. Use values below 1.0 for additional caution. |
| `ADAPTIVE_SCALING` | `false` | When `true`, the bot scales down position sizes after losses and scales up after wins. |

### 4.5 Market Filters

These define which markets the bot will even consider trading.

| Variable | Default | Description |
|---|---|---|
| `UP_DOWN_ONLY` | `true` | When `true`, only 5-minute Up/Down crypto markets are scanned. This is the primary recommended mode. |
| `TARGET_SYMBOLS` | `BTC,ETH,SOL,HYPE` | Comma-separated list of crypto symbols to scan. Only markets involving these symbols are considered. The fourth element, HYPE, is Hyperliquid's native token — a market of considerable occult interest. |
| `MIN_LIQUIDITY_USD` | `200` | Minimum on-book liquidity in USD. Markets with less than this are ignored — they are too thin to execute cleanly. |
| `MIN_VOLUME_USD` | `100` | Minimum 24-hour volume in USD. Low-volume markets signal disinterest and are avoided. |
| `MAX_CONCURRENT_POSITIONS` | `3` | Maximum number of simultaneously open positions. Caps portfolio exposure. |
| `MIN_BALANCE_USD` | `0.3` | Minimum wallet USDC balance before trading halts. The bot stops trading to preserve gas money. |
| `MIN_TRADE_SIZE_USD` | `1.0` | Minimum trade size (also caps Kelly minimum from below). |

### 4.6 Risk Management

The bot's circuit breakers. These stop trading when things go wrong.

| Variable | Default | Description |
|---|---|---|
| `MAX_DAILY_RISK_USD` | Derived | Maximum total capital at risk per day. Accumulates across all open positions. |
| `AGGRESSIVE_MODE` | `false` | When `true`, some conservative filters are relaxed. Only for experienced operators who understand the consequences. |
| `DAILY_RISK_RESET_HOUR` | `0` | UTC hour at which the daily risk counter resets. Default midnight UTC. |
| `TRADE_JOURNAL_PATH` | auto | Path for the trade journal CSV file. All executed trades are logged here. |
| `DAILY_SUMMARY_TXT` | auto | Path for the daily P&L summary text file. |

*Internal risk thresholds (not configurable via env, set in code):*
- **Maximum daily loss:** $25 USD (circuit breaker halts trading for the day)
- **Maximum drawdown:** 25% of peak balance (triggers halt)
- **Consecutive losses before pause:** 4 (with adaptive cooldown)
- **Maximum category exposure:** 50% of portfolio

### 4.7 Timing & Scan Intervals

| Variable | Default | Description |
|---|---|---|
| `SCAN_INTERVAL_SECONDS` | `12` | How frequently the scanner runs, in seconds. Range: 5–300. Values below 10 risk hitting RPC rate limits. |
| `POLYGON_RPC_URL` | `https://polygon-rpc.com` | Fallback Polygon RPC endpoint (used only if Alchemy is not configured). |

### 4.8 Timing Filter & Sweet-Spot Window (V6)

*The most dangerous moment in a five-minute market is not the final second — it is the first two hundred. Enter too early and you are guessing. Enter too late and the transaction will not confirm before resolution. The sweet spot is a narrow band, and this filter enforces it with iron discipline.*

The V6 Timing Filter restricts entries to a configurable window of time before each slot closes. Only trades with extraordinarily strong edge are permitted outside this window.

| Variable | Default | Description |
|---|---|---|
| `TIMING_MIN_SECONDS_BEFORE_CLOSE` | `75` | The outer boundary of the sweet spot. Trades are blocked if more than this many seconds remain, unless edge is very strong. |
| `TIMING_MAX_SECONDS_BEFORE_CLOSE` | `20` | The inner boundary. Trading ceases below this threshold — execution risk is too high. |
| `TIMING_STRONG_EDGE_OVERRIDE` | `0.045` | Edge percentage (4.5%) above which the timing filter is bypassed. A signal this powerful may enter at any time. |

**How it works:** For each candidate market, the scanner parses the slot timestamp from the market slug (e.g. `btc-updown-5m-1743000000`) and computes how many seconds remain. If the current time is outside `[TIMING_MAX, TIMING_MIN]`, the trade is skipped — unless edge exceeds `TIMING_STRONG_EDGE_OVERRIDE`.

### 4.9 Edge-Bucketed Kelly (V6)

*A flat Kelly multiplier treats a 52% confidence signal identically to a 78% confidence signal. This is an affront to mathematics. The Bucketed Kelly system corrects this heresy.*

When `USE_BUCKETED_KELLY=true`, the Kelly fraction is drawn from a graduated table keyed to edge strength:

| Edge Bucket | Kelly Fraction | Philosophy |
|---|---|---|
| Very weak (< 1.5%) | 0.15× | Near-abstention. The oracle speaks in whispers. |
| Weak (1.5–3%) | 0.25× | Conservative participation. |
| Moderate (3–5%) | 0.40× | Standard half-Kelly territory. |
| Strong (5–7%) | 0.60× | Confident conviction. |
| Very strong (> 7%) | 0.75× | Maximum permitted aggression. |

The Kelly parameters themselves are now **configurable via environment variable** rather than hardcoded, so you may calibrate them to your own observed trade results:

| Variable | Default | Description |
|---|---|---|
| `USE_BUCKETED_KELLY` | `true` | Enable edge-bucketed position sizing. When `false`, falls back to flat `KELLY_MULTIPLIER`. |
| `KELLY_AVG_WIN_PCT` | `0.07` | Your observed average win size as a fraction (0.07 = 7%). Calibrate this from your trade journal. |
| `KELLY_AVG_LOSS_PCT` | `0.04` | Your observed average loss size as a fraction (0.04 = 4%). Calibrate this from your trade journal. |

### 4.11 Hyperliquid Engine

*The Binance WebSocket feeds the left eye. The Hyperliquid WebSocket feeds the right. With both eyes open, the bot perceives depth that neither could see alone.*

The Hyperliquid Engine (`src/polybot/hyperliquid_engine.py`) connects to `wss://api.hyperliquid.xyz/ws` and derives three categories of signal:

1. **Trade Flow** — Accumulates buy and sell volume in rolling 10-second windows. When $500K+ flows in one direction in ten seconds, it constitutes a Big Flow event (`confidence: 0.78`).
2. **Order Flow Imbalance (OFI)** — Monitors L2 bid/ask depth. A ratio above 1.3 signals strong buying pressure; below 0.7 signals strong selling pressure.
3. **Latency Arbitrage** — Detects when Hyperliquid's perpetual mid-price has diverged ≥ 0.30% from Polymarket's implied probability, signalling a likely imminent correction.

Supported assets: `BTC`, `ETH`, `SOL`, `XRP`, `HYPE`.

| Variable | Default | Description |
|---|---|---|
| `HYPERLIQUID_ENABLED` | `false` | Start the Hyperliquid engine at boot. Also activates automatically when `HYPE 45` mode is selected. |

### 4.12 Full Redeemer (V89)

The Full Redeemer is an independent background daemon that scans the blockchain for all positions that have resolved in your favour, and redeems them automatically. It operates independently of the main trading loop and survives restarts.

| Variable | Default | Description |
|---|---|---|
| `FULL_REDEEM_ENABLED` | `false` | Enable the Full Redeemer background task. Set to `true` in production — you will miss winnings without it. |
| `FULL_REDEEM_INTERVAL_SECONDS` | `45` | How frequently the redeemer scans for redeemable positions. Range: 10–600 seconds. |
| `MIN_REDEEM_BALANCE` | `0.08` | Minimum USDC for the redeemer to attempt a redemption (only gas needed, much lower than trading threshold). |
| `REDEEM_GAS_BUFFER_PERCENT` | `30` | Gas price buffer for redeem transactions, as a percentage above estimated gas. |
| `STARTUP_REDEEM_ALL` | `true` | On startup, scan the wallet for ALL redeemable positions and collect them. Catches positions that were won during downtime. |

Manual trigger via API:
```
POST /api/force_full_redeem
GET  /api/full_redeemer_status
```

### 4.13 Backtesting & Hyperparameter Optimisation

| Variable | Default | Description |
|---|---|---|
| `BACKTEST_DAYS` | `30` | Number of historical days to backtest over. |
| `BACKTEST_MODE` | `edge` | Backtest depth. `edge` = edge engine only (fast), `full` = full execution simulation (slow), `walkforward` = rolling window validation. |
| `BACKTEST_MIN_LIQUIDITY` | `200` | Minimum liquidity filter applied during backtesting. |
| `BACKTEST_COMMISSION_BPS` | `10` | Commission in basis points applied to simulated trades (Polymarket taker fee is 10 bps). |
| `BACKTEST_OUTPUT_CSV` | `backtest_results_edge.csv` | Output file for backtest results. |
| `HYPEROPT_ENABLED` | `false` | Enable Optuna hyperparameter optimisation. When `true` and `MODE=hyperopt`, the bot searches for optimal parameters. |
| `HYPEROPT_PARAMS` | (see config) | JSON object defining the hyperparameter search space. |
| `WALKFORWARD_WINDOWS` | `4` | Number of rolling windows for walk-forward validation. |
| `OPTUNA_SAMPLER` | `tpe` | Optuna sampling algorithm. Options: `tpe`, `cmaes`, `random`, `grid`. TPE is recommended. |
| `OPTUNA_N_TRIALS` | `30` | Number of optimisation trials. More trials = better results but longer runtime. |
| `OPTUNA_DIRECTION` | `maximize` | Optimisation direction. `maximize` for profit/Sharpe, `minimize` for loss. |
| `OPTUNA_VIZ_ENABLED` | `true` | Generate visualisation plots of the optimisation landscape. |
| `OPTUNA_VIZ_FORMATS` | `html,png` | Output formats for visualisation. |
| `AUTO_APPLY_BEST` | `true` | Automatically apply the best-found parameters after optimisation completes. |
| `VIZ_DIR` | `/app/static/optuna_viz` | Directory for optimisation visualisation output files. |

### 4.14 Copy Trading & Whale Tracking

PolyBot can shadow the positions of known profitable traders ("whales") on Polymarket.

| Variable | Default | Description |
|---|---|---|
| `COPY_TRADER_ADDRESSES` | `""` | Comma-separated list of Polymarket wallet addresses to copy. |
| `COPY_SIZE` | `10.0` | Base trade size in USD for copy trades. |
| `TRADE_MULTIPLIER` | `1.0` | Size multiplier applied to copy trades. |
| `TIERED_MULTIPLIERS` | `""` | Tiered multiplier rules, e.g. `1-100:2.0,100-1000:0.5,1000+:0.1`. Scales copy size based on the whale's own position size. |

The `MODE=copy` activates full copy trading mode. The whale tracker additionally queries the Gamma API to identify top-performing wallets automatically.

### 4.15 The Volatility Schedule

*The market breathes. It contracts and expands in rhythms aligned with the opening of the great exchanges. Those who understand these rhythms trade in harmony with them.*

The Volatility Schedule tracks known high-volatility windows and adjusts trading behaviour accordingly:

| Window | UTC Time | Intensity | Reason |
|---|---|---|---|
| US Market Open | 14:20–15:00 | 1.8× | New York equity open, biggest daily spike |
| US Market Close | 20:50–21:10 | 1.4× | Equity close creates crypto movement |
| London Open | 07:50–08:20 | 1.3× | European capital enters the market |
| Asia Open | 00:00–01:00 | 1.2× | Tokyo and Hong Kong sessions begin |
| Funding Rate Resets | 23:55, 07:55, 15:55 | 1.3× | Liquidation cascades cluster at 8-hour funding resets |

**Volatility Modes** (set via `VOLATILITY_MODE`):
- `aggressive` — Trade only during high-volatility windows, with 1.5× position sizing
- `adaptive` — Trade always but scale sizing by the current volatility regime (default)
- `passive` — Trade always, ignore the schedule entirely

Enable with `VOLATILITY_SCHEDULE=true`.

### 4.16 Proxy Settings (Geo-Bypass)

Polymarket is geo-restricted in certain jurisdictions. The bot supports SOCKS5 proxies and proxy rotation.

| Variable | Default | Description |
|---|---|---|
| `SOCKS5_PROXY_HOST` | `""` | SOCKS5 proxy hostname. Leave empty to disable. |
| `SOCKS5_PROXY_PORT` | `0` | SOCKS5 proxy port. |
| `SOCKS5_PROXY_USER` | `""` | SOCKS5 proxy username (if authentication required). |
| `SOCKS5_PROXY_PASS` | `""` | SOCKS5 proxy password. Treated as a secret. |
| `PROXY_POOL` | `""` | Comma-separated list of proxy URLs for rotation. |

### 4.17 Dashboard Authentication

| Variable | Default | Description |
|---|---|---|
| `DASHBOARD_PASSWORD` | `""` | Password for the web dashboard. If empty, authentication is disabled (not recommended in production). The username is always `admin`. |

---

## 5. Operating Modes — The Fourteen Schools of Thought

*Where once there were eight blunt instruments, now there are fourteen scalpels. The `MODE` environment variable selects the base operating loop. But the true power lies in the Mode Selector dashboard (`/modes`), which resolves each UI mode into a full `StrategyConfig` — specifying not merely what loop to run, but which direction to trade, which signal sources to consult, what timing windows to enforce, and how aggressively to size. This is the grimoire of grimoires.*

### 5.1 Base Modes (set via `MODE` environment variable)

The `MODE` environment variable selects the operating mode's backend loop. There are eight fundamental modes:

| Mode | Description |
|---|---|
| `updown` | **Primary Mode.** Scans and trades 5-minute Up/Down crypto markets on BTC, ETH, SOL, and HYPE. This is the mode for which all other systems were built. |
| `signal` | Signal-based trading on general Polymarket prediction markets using the full signal engine. Broader market scope than `updown`. |
| `copy` | Copy trading mode. Mirrors the on-chain positions of designated whale addresses. |
| `arbitrage` | Arbitrage scanning mode. Identifies price discrepancies between correlated markets and exploits them. |
| `sniper` | Sniper mode. Waits for specific mispricing events and enters with high confidence immediately when detected. |
| `all` | Runs all available trading strategies simultaneously. Maximum aggression. Not for the faint-hearted. |
| `backtest` | Backtesting mode. No live trades are placed. The bot replays historical data and reports performance metrics. |
| `hyperopt` | Hyperparameter optimisation mode. Uses Optuna to search for optimal configuration parameters via Bayesian optimisation. |

### 5.2 Strategy Profiles — The Fourteen Grimoires

These are the fine-grained strategy profiles selectable from the Mode Selector UI (`/modes`) or via `POST /api/mode`. Each resolves into a complete `StrategyConfig` (`src/polybot/mode_strategies.py`) that overrides timing, direction, signal sources, and risk parameters:

| UI Mode | Label | Base Loop | Description |
|---|---|---|---|
| `sniper45` | Sniper 45 | sniper | Last 45 seconds of each slot. Sweet spot: 70–85¢ implied prices. Single Binance signal source. |
| `sniper30` | Sniper 30 | sniper | Last 30 seconds only. Tighter window, lower minimum confidence (0.58). Maximum urgency. |
| `sniper60` | Sniper 60 | sniper | Last 60 seconds. Wider window, higher minimum confidence (0.65). More selective entries. |
| `hype45` | HYPE 45 | sniper | Last 45 seconds with Hyperliquid engine active. Trades BTC, ETH, SOL, XRP, HYPE. Both Binance and Hyperliquid signals consulted. |
| `demonscalp` | DemonScalp | sniper | Last 30 seconds. Low minimum confidence (0.55). Hyperliquid active. Consensus *not* required. Small Kelly multiplier (0.3×). Maximum aggression, maximum risk. |
| `coinbase` | Coinbase | signal | Signal-only mode using Binance. Minimum confidence 0.62. Trades during US market hours only (14:00–22:00 UTC). |
| `full_consensus` | Full Consensus | signal | Requires both Binance AND Hyperliquid to agree before any entry. Conservative (min confidence 0.70). High-conviction trades only. |
| `down_only` | Down Only | updown | Standard updown loop but restricted to DOWN side only. Useful in bearish regimes. |
| `updown` | Up/Down | updown | Standard full updown mode. Both directions. Default strategy. |
| `arbitrage` | Arbitrage | arbitrage | Arbitrage scanner. No directional bias. |
| `signal` | Signals | signal | Full signal engine across general markets. Minimum confidence 0.60. |
| `signal247` | Sig 24/7 | signal | Signal mode running continuously with no hour restrictions. Does not skip low-volatility periods. |
| `autopred` | AutoPred | signal | High-confidence signal mode (min 0.72). Kelly multiplier 0.6×. Trades UP side only. Auto-prediction filtering. |
| `reverse` | Reverse | signal | Contrarian mode. Fades the crowd: bets DOWN when signals say UP, bets UP when signals say DOWN. For use when the market is known to be wrong. |

---

## 6. The Mode Selector — Switching Grimoires at Runtime

*The ancients were forced to restart their rituals from the beginning whenever they wished to change their approach. You are not so limited.*

The Mode Selector (`static/modes.html`) is a full-screen web UI served at `/modes`. It presents all fourteen strategy grimoires as selectable cards, each showing its label, description, and active indicator. Clicking a card sends a `POST /api/mode` request and switches the bot's operating strategy **immediately, without restart**.

**Access:** `http://localhost:8080/modes` (or your Railway deployment URL + `/modes`)

**API:**

```
GET  /api/mode          → Returns current active mode and sub-config
POST /api/mode          → Switches mode (body: {"ui_mode": "hype45", "mode": "sniper", "snipe_window": 45})
```

The current mode persists in memory until the bot is restarted (at which point `MODE` from `.env` takes effect again). For permanent changes, update `MODE` in your environment file.

---

## 7. The Decision Engine — How Trades Are Summoned

The heart of PolyBot is the **Edge Engine** (`src/polybot/edge_engine.py`), which consults the **Signal Engine** (`src/polybot/signal_engine.py`). Here is the precise incantation sequence:

### Step 1 — Market Discovery
The scanner queries `https://clob.polymarket.com` for active markets. For `updown` mode, it filters for:
- Market description matching the 5-minute Up/Down pattern
- Token symbols matching `TARGET_SYMBOLS`
- Minimum liquidity ≥ `MIN_LIQUIDITY_USD`
- Minimum 24h volume ≥ `MIN_VOLUME_USD`

### Step 2 — Price Oracle Consultation
For each candidate market, live price and volume data is fetched from Binance. The Edge Engine also queries Polymarket's current implied probability (derived from best-bid/ask prices).

### Step 3 — Signal Computation
Five indicators are computed and weighted:

| Indicator | Weight | What It Measures |
|---|---|---|
| MA Crossover | 27% | 5-period vs 20-period moving average relationship |
| RSI (14-period) | 27% | Relative Strength Index. Oversold (<30) favours Up; Overbought (>70) favours Down |
| MACD (12/26/9) | 23% | Moving Average Convergence/Divergence trend signal |
| Momentum | 13% | Raw price momentum over recent candles |
| Volume Momentum | 10% | Whether volume is expanding in the direction of the move |

The weighted sum produces a confidence score from 0 to 100.

### Step 4 — EV Calculation
Expected Value is computed as:

```
EV = (P_win × avg_win_pct) - (P_lose × avg_loss_pct)
```

Where:
- `P_win` is the bot's estimated win probability (from signal confidence)
- `avg_win_pct` = `KELLY_AVG_WIN_PCT` (default 7%) — configurable from your trade history
- `avg_loss_pct` = `KELLY_AVG_LOSS_PCT` (default 4%) — configurable from your trade history

If `EV < MIN_EV`, the trade is rejected.

### Step 5 — Kelly Sizing
Position size is calculated as:

```
Kelly% = (P_win / avg_loss_pct) - (P_lose / avg_win_pct)
Kelly_USD = Kelly% × KELLY_MULTIPLIER × balance
Kelly_USD = clamp(Kelly_USD, MIN_TRADE_USD, MAX_POSITION_USD)
```

When `USE_BUCKETED_KELLY=true` (the default), the effective multiplier is drawn from the bucketed table described in §4.9 rather than the flat `KELLY_MULTIPLIER`. The two approaches are mutually exclusive — bucketed Kelly supersedes flat Kelly when enabled.

### Step 6 — Risk Checks
Before execution, the Risk Manager (`src/polybot/risk_manager.py`) verifies:
- Daily risk budget not exceeded
- Concurrent position limit not breached
- Daily loss limit not breached
- Drawdown circuit breaker not active
- Consecutive loss circuit breaker not active

### Step 7 — On-Chain Execution
If all checks pass, the On-Chain Executor (`src/polybot/onchain_executor.py`) constructs a signed EIP-712 order, submits it to the Polymarket CLOB API, and confirms the on-chain transaction.

---

## 8. The Hyperliquid Engine — The Second Eye of Providence

*In all great traditions of divination, it is known that a single oracle may be deceived, but two independent oracles agreeing constitutes a vision of uncommon clarity. The Hyperliquid Engine is the second oracle.*

The Hyperliquid Engine (`src/polybot/hyperliquid_engine.py`) connects to the Hyperliquid decentralised perpetuals exchange via WebSocket and processes three signal categories:

### 8.1 Trade Flow Analysis

Hyperliquid streams every trade in real time. The engine accumulates buy and sell volume in 10-second rolling windows. When the net directional flow exceeds $500,000 USD in a single window, it classifies the event as a **Big Flow** signal with 78% confidence. This level of urgency correlates with significant short-term price continuation.

### 8.2 Order Flow Imbalance (OFI)

The engine subscribes to Level-2 order book snapshots. At each update, it computes:

```
OFI = total_bid_size / total_ask_size
```

- OFI > 1.3 → Strong buy pressure (confidence: 65%)
- OFI > 0.7 but < 1.3 → Weak buy pressure (confidence: 55%)
- OFI < 0.7 → Sell pressure (confidence: 65%)

Hyperliquid's thresholds are deliberately calibrated lower than Binance's (1.5 / 0.8) to account for the exchange's lower absolute liquidity.

### 8.3 Latency Arbitrage

The engine continuously monitors the spread between Hyperliquid's perpetual mid-price and Polymarket's implied probability. When the gap exceeds **0.30%** (and the Polymarket price is in the 43–57¢ range — neither extreme), it generates a latency arb signal (confidence: 68%). The logic: Hyperliquid is a faster, more liquid market. Its price is likely to be correct, and Polymarket will follow.

### 8.4 Signal Integration

When `HYPERLIQUID_ENABLED=true` or the active strategy profile includes `use_hyperliquid=True`, the Hyperliquid signal is combined with the Binance signal. The integration mode depends on the strategy:
- **Standard** — Either source confirming the direction is sufficient
- **Consensus required** (`signal_consensus_required=True`) — Both sources must agree, or the trade is blocked

Monitor engine status: `GET /api/hype_status`

---

## 9. The Full Redeemer — Harvesting the Fruits of Victory

*A victory uncollected is a victory surrendered.*

The Full Redeemer (`src/polybot/full_redeemer.py`) is an independent background service that ensures every winning position is redeemed. It operates by:

1. **On-chain scanning:** Queries the Polygon blockchain directly for CTF token balances in your wallet.
2. **Data API scanning:** Queries Polymarket's Data API for resolved markets where you hold positions.
3. **Cross-referencing:** Identifies positions that have resolved YES (in your favour) but have not yet been redeemed.
4. **Automatic redemption:** Calls the appropriate redemption function on the CTF Exchange or NegRisk Exchange smart contract.

Enable with `FULL_REDEEM_ENABLED=true`. The redeemer runs every `FULL_REDEEM_INTERVAL_SECONDS` (default 45 seconds) as a background asyncio task.

**Startup Sweep (`STARTUP_REDEEM_ALL=true`):** When the bot starts, it performs a one-time comprehensive scan of ALL your on-chain positions and redeems everything eligible. This catches any winnings accumulated during downtime.

Manual trigger:
```bash
curl -X POST http://localhost:8080/api/force_full_redeem
```

---

## 10. The Dashboard — Watching the Stars Move

PolyBot runs a web dashboard on port `8080`. This is your window into the machine.

**Access:** `http://localhost:8080` (or your Railway deployment URL)

**Authentication:** If `DASHBOARD_PASSWORD` is set, you must log in with username `admin` and your configured password.

**Dashboard Pages:**

| `/modes` | Mode Selector UI — visual interface to switch between all fourteen strategy grimoires |
| `/` or `/dashboard` | Main dashboard showing live P&L, position status, risk metrics, and recent activity |
| `/api/health` | Health check endpoint. Returns `{"status": "ok"}` if the bot is running. |
| `/api/pnl` | Current P&L summary: total profit/loss, win rate, number of trades |
| `/api/positions` | All currently open positions with entry prices and current values |
| `/api/status` | Full bot status: mode, scan counts, last signal, last trade, uptime |
| `/api/risk` | Current risk metrics: daily risk used, circuit breaker state, drawdown |
| `/api/rpc-status` | RPC endpoint health and latency metrics |
| `/api/next_scan` | Time until next scanner cycle |
| `/api/risk_status` | Detailed risk manager state |
| `/api/rpc_health` | Detailed RPC health check |
| `/api/allowance_status` | Current USDC allowance on Polymarket contracts |
| `/api/full_redeemer_status` | Full Redeemer state: last run, positions found, amounts redeemed |
| `/api/mode` | Current active strategy mode and sub-configuration |
| `/api/hype_status` | Hyperliquid engine status: prices, trade counts, signals per asset |
| `/api/backtest-report` | Latest backtest results |
| `/api/strategy-comparison` | Comparison of strategy variants from backtesting |
| `/api/production-status` | Production readiness indicators |
| `/api/production-safety` | Safety checks: DRY_RUN state, circuit breakers, balance |
| `/logs` | Tail of the bot's log output |

---

## 11. API Reference — The Sacred Endpoints

In addition to the GET endpoints listed in the Dashboard section, the following POST endpoints are available:

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/mode` | Returns current active strategy mode and sub-configuration |
| `POST` | `/api/mode` | Switch strategy at runtime. Body: `{"ui_mode": "hype45", "mode": "sniper", "snipe_window": 45}` |
| `GET` | `/api/hype_status` | Hyperliquid engine status — prices, OFI, signals, trade counts per asset |
| `POST` | `/api/force_full_redeem` | Immediately triggers a full redemption scan, regardless of interval schedule |
| `POST` | `/apply-best-params` | Applies the best hyperparameters found by the most recent Optuna optimisation run |
| `POST` | `/api/trigger-backup` | Triggers a manual backup of the database and state files |

---

## 12. The Volatility Schedule — Trading in Harmony with the Cosmic Clock

*The uninitiated trader enters the market at random hours, wondering why fortune does not favour them. The adept knows: the market breathes according to the clock of human institutions.*

The Volatility Schedule module (`src/polybot/volatility_schedule.py`) encodes the recurring windows of heightened market activity and adjusts the bot's behaviour accordingly.

**Configuration:**
```
VOLATILITY_SCHEDULE=true
VOLATILITY_MODE=adaptive   # aggressive | adaptive | passive
```

**What happens during a high-volatility window:**
- In `aggressive` mode: Only trades when intensity ≥ 1.2. Position sizes scaled by the window intensity.
- In `adaptive` mode: Always trades, but scales position size by current intensity. During the US Market Open (intensity 1.8), positions are 80% larger than baseline.

**What happens outside volatility windows:**
- In `aggressive` mode: The bot idles, watching but not trading.
- In `adaptive` mode: The bot trades at baseline sizing.

This feature combines beautifully with the Volatility Regime detector (`src/polybot/volatility_regime.py`), which measures real-time market volatility via ATR and classifies the current regime as LOW, NORMAL, or HIGH.

---

## 13. Backtesting — Consulting the Ancestors

*Before committing capital to any strategy, the prudent student tests it against the record of history.*

**Run a backtest:**
```bash
MODE=backtest python -m polybot
# or
python -m polybot backtest
```

**Backtest modes:**

- `edge` (default) — Fast. Tests only the Edge Engine signal quality against historical Polymarket data. No execution simulation.
- `full` — Slow. Simulates the complete execution pipeline including Kelly sizing, slippage, fees, and position limits.
- `walkforward` — Rolling window validation. Splits the backtest period into `WALKFORWARD_WINDOWS` segments, optimising on the first and testing on each subsequent window.

**Output:**
- Console summary: total return, Sharpe ratio, win rate, max drawdown, profit factor
- CSV file (`BACKTEST_OUTPUT_CSV`) with per-trade details
- API endpoint `/api/backtest-report` for JSON results

---

## 14. Hyperparameter Optimisation — Seeking the Perfect Proportion

*The parameters you set are not the optimal parameters. They are merely the starting point. Let the machine find the truth.*

PolyBot integrates [Optuna](https://optuna.org), a Bayesian hyperparameter optimisation framework, to automatically search for the parameter configuration that maximises your chosen objective (typically Sharpe ratio or total return).

**Run optimisation:**
```bash
HYPEROPT_ENABLED=true MODE=hyperopt OPTUNA_N_TRIALS=50 python -m polybot
```

**What is optimised:**
- `MIN_EV` threshold
- `MIN_EDGE_PERCENT`
- `KELLY_MULTIPLIER`
- `MIN_LIQUIDITY_USD`
- `MIN_CONFIDENCE_FILTER`
- And any additional parameters defined in `HYPEROPT_PARAMS`

**Sampler options:**
- `tpe` — Tree-structured Parzen Estimator (recommended, best for most problems)
- `cmaes` — Covariance Matrix Adaptation Evolution Strategy (good for continuous parameters)
- `random` — Random search (baseline comparison)
- `grid` — Exhaustive grid search (only feasible for small search spaces)

**Results:**
When `AUTO_APPLY_BEST=true`, the best-found parameters are automatically written to the configuration and applied on the next trading session. You can also apply them manually:
```bash
curl -X POST http://localhost:8080/apply-best-params
```

Visualisations of the optimisation landscape are written to `VIZ_DIR` (default `/app/static/optuna_viz`) if `OPTUNA_VIZ_ENABLED=true`.

---

## 15. Deployment on Railway — The Cloud Temple

Railway is the recommended deployment platform. It provides persistent environment variables, automatic deploys from GitHub, and a URL for your dashboard.

### Step 1 — Create a New Project

Go to [railway.app](https://railway.app) and create a new project from your GitHub repository.

### Step 2 — Set Environment Variables

In Railway's Dashboard → Variables, add **at minimum**:

```
POLYGON_PRIVATE_KEY    = 0x<your_private_key>
WALLET_ADDRESS         = 0x<your_wallet_address>
ALCHEMY_API_KEY        = <your_alchemy_key>
DRY_RUN                = true          # Start here. Test first.
AUTO_EXECUTE           = false         # Start here. Test first.
MODE                   = updown
```

> 🔐 Railway encrypts these variables and never exposes them in logs. They are stored as secrets.

### Step 3 — Deploy

Push to your connected GitHub branch. Railway detects the `nixpacks.toml` (or `Dockerfile`) and deploys automatically.

### Step 4 — Verify

Check the logs in Railway's dashboard. You should see:
```
[STARTUP] PolyBot starting in DRY_RUN mode
[SCANNER] Scanning 2395 markets...
[EDGE] Signal: BTC Up | Confidence: 71.3 | EV: 0.014 | DRY_RUN: no trade placed
```

### Step 5 — Go Live (When Ready)

When you are satisfied with dry-run behaviour:
```
DRY_RUN      = false
AUTO_EXECUTE = true
```

Redeploy. The bot will now trade with real money.

### Docker Deployment (Alternative)

```bash
docker-compose up -d
```

The `docker-compose.yml` file is pre-configured for local deployment. Set your environment variables in a `.env` file before running.

The `Dockerfile` uses a multi-stage build optimised for production. The image is based on Python 3.11 slim.

---

## 16. Running Locally — The Private Laboratory

*Before unleashing anything upon the world, the adept experiments in their private laboratory.*

```bash
# Install dependencies
pip install -r requirements.txt

# Copy and edit the config
cp env.example .env
# Edit .env: set your keys, keep DRY_RUN=true for safety

# Run the bot
python -m polybot

# Or with the CLI
python -m polybot --help
python -m polybot --mode updown --dry-run
python -m polybot redeem-all    # Manually redeem all winning positions
python -m polybot check-wallet  # Check balance and allowances
python -m polybot backtest      # Run a backtest
```

The dashboard will be available at `http://localhost:8080`.

---

## 17. The Test Suite — Proving the Formulas

*An untested formula is a prayer. A tested formula is a law.*

The test suite covers all major components:

```bash
# Run all tests
pytest tests/ -v

# Run with coverage report
pytest tests/ --cov=src/polybot --cov-report=html

# Run a specific test module
pytest tests/test_risk_manager.py -v
pytest tests/test_backtester.py -v
pytest tests/test_credentials_manager.py -v
```

**Test modules included:**

| Module | What It Tests |
|---|---|
| `test_backtester.py` | Backtesting engine accuracy and edge case handling |
| `test_credentials_manager.py` | Credential loading, derivation, and security (no disk caching) |
| `test_dashboard.py` | Dashboard endpoint responses and authentication |
| `test_full_redeemer.py` | Full Redeemer scan logic and redemption flow |
| `test_geo.py` | Geographic restriction detection and proxy bypass |
| `test_hourly_risk_regime.py` | Hourly risk regime detection and switching |
| `test_indicators.py` | Technical indicator calculations (RSI, MACD, MA) |
| `test_models.py` | Pydantic model validation and serialisation |
| `test_onchain_executor.py` | On-chain order construction and signing |
| `test_optimizer.py` | Kelly Criterion calculations and position sizing |
| `test_pnl_tracker.py` | P&L tracking accuracy |
| `test_production_dashboard_endpoints.py` | Production-specific endpoint integration tests |
| `test_proxy.py` | SOCKS5 proxy rotation and failover |
| `test_redeem_all.py` | Bulk redemption logic |
| `test_redeem_only.py` | REDEEM_ONLY mode verification |
| `test_redeem_stuck_positions.py` | Stuck position detection and recovery |
| `test_retries.py` | Retry logic and exponential backoff |
| `test_risk_manager.py` | Circuit breaker logic and risk limit enforcement |
| `test_rpc_manager.py` | RPC failover and load balancing |
| `test_scanner.py` | Market scanning and filter application |
| `test_signal_engine.py` | Signal computation and weighting |
| `test_simple_trend_filter.py` | Trend filter logic |
| `test_smart_timing.py` | Timing and scheduling logic |
| `test_solana_bridge.py` | Solana bridge auto-funding |
| `test_terminal_logger.py` | Terminal output formatting |
| `test_updown_crypto.py` | Up/Down crypto market specific logic |
| `test_auth_and_persistence.py` | Dashboard authentication and state persistence |
| `test_mode_strategies.py` | Strategy profile resolution, direction filtering, and mode selector logic |
| `test_timing_and_kelly.py` | V6 Timing Filter sweet-spot enforcement and Edge-Bucketed Kelly sizing |

CI runs automatically on every push via GitHub Actions (`.github/workflows/ci.yml`).

---

## 18. Security Architecture — The Wards and Seals

*The occultist who leaves their grimoire unguarded invites disaster. The trader who leaves their private key exposed invites ruin. These are equivalent catastrophes.*

### Credential Management (V91 Security Fix)

Credentials are **never cached to disk**. In earlier versions, derived credentials were written to `/tmp/polymarket_creds.json` — this was a security vulnerability and has been eliminated. Now:

1. Credentials are loaded from environment variables (highest priority)
2. If env vars are not present, credentials are derived fresh from `POLYGON_PRIVATE_KEY` on every startup
3. No file system caching of any kind

```python
CREDS_FILE = None  # V91: Credentials no longer cached to disk (security fix)
```

### Dashboard Security

- HTTP Basic Authentication (configurable via `DASHBOARD_PASSWORD`)
- Rate limiting on authentication failures (IP-based lockout)
- Security headers middleware (`X-Content-Type-Options`, `X-Frame-Options`, `X-XSS-Protection`)
- No sensitive data (keys, secrets) ever appears in API responses or logs

### Logging Security

All fields defined as `SecretStr` in the configuration are never logged in plain text. Pydantic ensures they display as `**********` in debug output.

### Private Key Handling

The private key is accepted with or without the `0x` prefix and is validated on load. It is stored exclusively in memory as a `SecretStr` field and never written to any file.

---

## 19. Database — The Akashic Record

*All that occurs is recorded. The Akashic Record forgets nothing.*

PolyBot uses SQLite for persistence. The database file is `polybot.db` (hardcoded path, in the working directory). It is created automatically on first run.

**Tables:**

| Table | Contents |
|---|---|
| `trades` | All executed trades: market ID, direction, size, entry price, outcome |
| `positions` | Currently open positions with entry data |
| `pnl_daily` | Daily P&L summaries |
| `scan_log` | Scanner execution log with signals found |
| `risk_events` | Circuit breaker triggers and risk limit events |
| `kv_store` | Key-value store for bot state persistence across restarts |

**Backup:**
```bash
curl -X POST http://localhost:8080/api/trigger-backup
```
Creates a timestamped backup of the database in a `backups/` directory.

---

## 20. The Solana Bridge — Crossing the Dimensional Threshold

*For those who hold capital across multiple chains, the bridge is the passage between worlds.*

The Solana Bridge (`src/polybot/solana_bridge.py`) is an optional feature that automatically tops up your Polygon USDC balance when it falls below a threshold, by bridging funds from a Solana wallet.

**Flow:**
1. When Polygon USDC balance falls below `SOLANA_MIN_BALANCE_USDC` (default $20)
2. The bridge transfers `SOLANA_BRIDGE_AMOUNT` (default $50) from Solana to Polygon
3. Uses Jupiter DEX for token swapping on Solana
4. Uses Wormhole or Allbridge for the cross-chain transfer

**Requirements:**
- `SOLANA_PRIVATE_KEY` must be set
- The Solana wallet must hold sufficient SOL for gas and USDC for the bridge amount

**Configuration constants (hardcoded):**
- Solana RPC: `https://api.mainnet-beta.solana.com`
- Jupiter API: `https://quote-api.jup.ag/v6`
- Minimum bridge amount: $20 USDC
- Default bridge amount: $50 USDC
- Slippage tolerance: 50 bps (0.5%)

---

## 21. Troubleshooting — Exorcising the Demons

*When the ritual fails, do not despair. Consult the symptom, identify the cause, apply the remedy.*

### The bot starts but places no trades

**Symptoms:** Log shows signals found but `DRY_RUN: no trade placed`

**Remedy:** You are in dry-run mode (this is correct behaviour for testing). Set `DRY_RUN=false` and `AUTO_EXECUTE=true` to enable live trading.

---

### "Derived credentials are empty" error

**Symptoms:** Bot fails to start with `[CREDS] Derived credentials are empty`

**Remedy:** Your `POLYGON_PRIVATE_KEY` is missing, empty, or invalid. Verify it is set correctly in Railway environment variables. It must start with `0x` and be 66 characters total (0x + 64 hex chars).

---

### RPC errors / slow execution

**Symptoms:** Log shows `[RPC] Rate limited` or `[RPC] Timeout`

**Remedy:** Set your `ALCHEMY_API_KEY`. Public RPCs are heavily rate-limited. Alchemy provides 300M compute units/month free, which is more than sufficient.

---

### USDC allowance error

**Symptoms:** Trades fail with `ERC20: transfer amount exceeds allowance`

**Remedy:**
```bash
python -m polybot approve-usdc
```
Or set `auto_approve_enabled=true` (the default). The bot will approve at startup automatically.

---

### Circuit breaker triggered

**Symptoms:** Log shows `[RISK] Circuit breaker ACTIVE — consecutive losses: 4`

**Remedy:** This is the safety system working correctly. The bot has experienced 4 consecutive losses and is pausing. Wait for the adaptive cooldown to expire, review your configuration, and consider raising `MIN_EV` or `MIN_CONFIDENCE_FILTER`.

---

### "No markets found" in scanner

**Symptoms:** Log shows `[SCANNER] 0 markets passed filters`

**Remedy:**
1. Check that `UP_DOWN_ONLY=true` and `MODE=updown`
2. Try lowering `MIN_LIQUIDITY_USD` to 100 temporarily
3. Verify Polymarket is accessible (check for geo-restriction if no proxy is configured)

---

### Dashboard not accessible

**Symptoms:** Browser shows connection refused on port 8080

**Remedy:**
- For local: Confirm the bot is running and check for port conflicts
- For Railway: Verify the `PORT` environment variable is set to `8080` in Railway's settings, or that Railway's public networking is enabled for your service

---

## 22. Migration — The Grand Migration Ritual

If you are migrating from an older version of PolyBot (v1 or v2), consult `MIGRATION.md` for the full procedure. The helper script automates most of the migration:

```bash
bash migrate_to_polybot2.sh
```

**Key changes in V3:**
- `src/polybot/credentials_manager.py` no longer caches credentials to disk (security fix)
- `src/polybot/config.py` now defaults `dry_run=True` (safe default)
- `min_ev` restored to `0.010` (was temporarily lowered to `0.005` during testing)
- New test file: `tests/test_auth_and_persistence.py`

---

## 23. Feature Compendium

A complete list of all implemented features:

**Core Trading**
- ✅ 5-minute Up/Down crypto market trading (BTC, ETH, SOL, HYPE)
- ✅ Multi-factor signal engine (MA, RSI, MACD, Momentum, Volume)
- ✅ Kelly Criterion position sizing (half-Kelly default)
- ✅ Edge-Bucketed Kelly (V6): adaptive sizing by signal strength
- ✅ Configurable Kelly win/loss parameters (`KELLY_AVG_WIN_PCT`, `KELLY_AVG_LOSS_PCT`)
- ✅ Expected Value filtering
- ✅ On-chain order execution via Polymarket CLOB API
- ✅ EIP-712 signed order construction

**Signal Sources**
- ✅ Binance WebSocket — primary signal source (MA, RSI, MACD, Momentum, Volume)
- ✅ Hyperliquid WebSocket engine — trade flow, OFI, latency arb signals (BTC, ETH, SOL, XRP, HYPE)
- ✅ Dual-source consensus mode (both sources must agree before entry)

**Timing & Filtering**
- ✅ V6 Timing Filter: sweet-spot window enforcement (75–20 seconds before close)
- ✅ Configurable strong-edge override for early entries
- ✅ Volatility Schedule (time-aware trading intensity)
- ✅ Volatility Regime detection (ATR-based)
- ✅ Hourly risk regime classification

**Strategy System**
- ✅ 14 named strategy profiles (`src/polybot/mode_strategies.py`)
- ✅ Mode Selector web UI (`/modes`) — switch strategies at runtime without restart
- ✅ Runtime mode switching via `POST /api/mode`
- ✅ Direction filter (up-only, down-only, both)
- ✅ Per-strategy Kelly overrides, confidence thresholds, asset lists, active hours
- ✅ Reverse/contrarian mode (fades the crowd)

**Risk Management**
- ✅ Daily loss circuit breaker
- ✅ Consecutive loss circuit breaker with adaptive cooldown
- ✅ Maximum drawdown circuit breaker
- ✅ Maximum concurrent positions limit
- ✅ Daily risk budget tracking
- ✅ REDEEM_ONLY emergency mode

**Redemption**
- ✅ Full Redeemer V89: independent background redemption daemon
- ✅ Startup sweep for all redeemable positions (V90)
- ✅ Manual redemption trigger via API
- ✅ Stuck position detection and recovery

**Automation & Scheduling**
- ✅ Configurable scan interval (5–300 seconds)
- ✅ Compounding mode (reinvests profits)

**Analytics & Optimisation**
- ✅ Backtesting engine (edge, full, walk-forward modes)
- ✅ Optuna hyperparameter optimisation
- ✅ Strategy comparison reports
- ✅ Trade journal CSV export
- ✅ Daily P&L summaries

**Advanced Trading**
- ✅ Copy trading / whale tracking
- ✅ Gamma API integration for top trader discovery
- ✅ Arbitrage scanner
- ✅ Sniper mode (last N seconds before slot close)
- ✅ Adaptive position scaling

**Infrastructure**
- ✅ FastAPI dashboard and REST API
- ✅ SQLite persistence (polybot.db)
- ✅ Database backup API
- ✅ RPC load balancing with failover
- ✅ SOCKS5 proxy support with rotation
- ✅ USDC auto-approval
- ✅ Solana bridge for auto-funding
- ✅ Terminal logger with coloured output
- ✅ Dashboard authentication (HTTP Basic Auth)
- ✅ Rate limiting and security headers

**Security**
- ✅ No credential disk caching (V91 fix)
- ✅ SecretStr for all sensitive config fields
- ✅ No secrets in logs
- ✅ Dashboard security headers

---

## 24. Version Chronicle

| Version | Highlights |
|---|---|
| V92 | Profit savings module made fully configurable via env vars. No hardcoded addresses. Disabled by default. |
| V6 (Profitability Upgrade) | Timing Filter (`TIMING_MIN/MAX_SECONDS_BEFORE_CLOSE`). Edge-Bucketed Kelly (`USE_BUCKETED_KELLY`). Configurable Kelly parameters (`KELLY_AVG_WIN_PCT`, `KELLY_AVG_LOSS_PCT`). |
| V5 (Strategy System) | `mode_strategies.py`: 14 named strategy profiles. Mode Selector UI (`/modes`). Runtime mode switching via API. Hyperliquid Engine (`hyperliquid_engine.py`): trade flow, OFI, latency arb. HYPE added to target symbols. |
| V91 | Security fix: credentials never cached to disk. `CREDS_FILE=None`. |
| V90 | `STARTUP_REDEEM_ALL` feature: comprehensive position sweep on every restart. |
| V89 | Full Redeemer introduced: independent background redemption daemon. |
| V78 | `MIN_BALANCE_USD` lowered to $0.30 for minimal-balance operation. |
| V3 (Fixed) | Safe defaults: `DRY_RUN=true`. `MIN_EV` restored to `0.010`. New `test_auth_and_persistence.py`. |
| V3 | Volatility Schedule, Optuna hyperopt, walk-forward backtesting, Sniper mode. |
| V2 | On-chain executor rewrite, Full Redeemer, dashboard, copy trading. |
| V1 | Initial release: signal-based trading on Polymarket Up/Down markets. |

---

## Blockchain Addresses (Polygon Mainnet — Hardcoded)

These are permanent contract addresses and do not require configuration:

| Contract | Address |
|---|---|
| USDC.e (Bridged USDC) | `0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174` |
| Native USDC | `0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359` |
| CTF Exchange | `0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E` |
| NegRisk Exchange | `0xC5d563A36AE78145C45a50134d48A1215220f80a` |
| NegRisk Adapter | `0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296` |
| Conditional Tokens (CTF) | `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045` |

**Chain ID:** 137 (Polygon Mainnet)
**Polymarket CLOB API:** `https://clob.polymarket.com`

---

*"The markets are neither kind nor cruel. They are indifferent, as all forces of nature are indifferent. It is the operator who brings intention. Configure well. Watch closely. Harvest with patience."*

*— Aleister Moltley*

---

**License:** See `LICENSE` file.
