# 🤠 PolyBot – She's Ready to Go All Night, Boys

**This here bot don't just talk purdy – she puts out fer real.** Soon as you hitch her up on Railway, she'll be slidin' into them 5-minute Up/Down crypto markets (BTC, ETH, SOL) faster'n a greased pig at a county fair.

## 🚀 How to Get 'Er Up (and Keep 'Er Goin')

### 1. Clone the Goods
```bash
# Grab ahold an' take 'er home
git clone https://github.com/YOUR_USERNAME/PolyBot.git
cd PolyBot
# Hitch 'er up via Railway CLI or GitHub Integration
```

### 2. Set Yer Knobs on Railway

These here are the dials'n switches that make 'er purr:

| Variable | Default | What She Does |
|----------|---------|---------------|
| `MODE` | `updown` | How she likes it (updown = quick 5min crypto) |
| `DRY_RUN` | `false` | `false` = she goes all the way, no teasin'! |
| `AUTO_EXECUTE` | `true` | Lets 'er go hands-free, if ya catch mah drift |
| `MIN_EV` | `0.015` | Won't get outta bed fer less'n 1.5% edge |
| `KELLY_MULTIPLIER` | `0.5` | Half-Kelly – she paces herself, ain't no jackrabbit |
| `MAX_POSITION_USD` | `50` | Won't blow more'n fifty bucks on one go |
| `MIN_TRADE_USD` | `5` | Won't even bother fer less'n a fiver |
| `MIN_LIQUIDITY_USD` | `200` | Likes 'er markets wet – at least $200 deep |
| `MIN_VOLUME_USD` | `100` | Needs a hunnerd bucks of action in the last day |
| `UP_DOWN_ONLY` | `true` | Only does the quick Up/Down stuff |
| `TARGET_SYMBOLS` | `BTC,ETH,SOL` | Which coins she's got 'er eye on |
| `LOG_LEVEL` | `DEBUG` | Tells ya every little thang she's doin' |
| `POLYGON_PRIVATE_KEY` | - | **REQUIRED**: Yer secret key – guard it like yer moonshine recipe |
| `WALLET_ADDRESS` | - | **REQUIRED**: Where she sends the goods |
| `ALCHEMY_API_KEY` | - | Recommended: makes 'er go faster, real smooth-like |

### 3. Redeploy
Once you done fiddlin' with them knobs, give 'er a good redeploy an' she'll start performin' right quick!

## 🔄 Full Redeemer V89 – Never Leave Money on the Table

**NEW!** The Full Redeemer scans for ALL redeemable positions on-chain, independent of the bot's internal state. This means:

- ✅ **Works after restart** – doesn't "forget" positions like the old auto-redeem
- ✅ **Finds ALL positions** – scans Polymarket Data API for everything you can redeem
- ✅ **Lower balance threshold** – only needs gas money (~$0.08), not the $1.2 trading minimum
- ✅ **Manual trigger** – hit `/api/force_full_redeem` anytime to cash out

### Full Redeemer Settings

| Variable | Default | What She Does |
|----------|---------|---------------|
| `FULL_REDEEM_ENABLED` | `false` | Enable background scan every 45s |
| `FULL_REDEEM_INTERVAL_SECONDS` | `45` | How often to look for redeemable positions |
| `MIN_REDEEM_BALANCE` | `0.08` | Min USDC needed (just for gas) |
| `REDEEM_GAS_BUFFER_PERCENT` | `30` | Extra gas price buffer |

### How to Use

1. **Enable auto-redeem in Railway:**
   ```
   FULL_REDEEM_ENABLED=true
   ```

2. **Or trigger manually via API:**
   ```bash
   curl -X POST https://your-bot.railway.app/api/force_full_redeem
   ```

3. **Check status:**
   ```bash
   curl https://your-bot.railway.app/api/full_redeemer_status
   ```

## ✅ Pre-Rodeo Checklist (Before She Bucks)

- [ ] `DRY_RUN=false` – no more pretendin', time fer the real thang
- [ ] `AUTO_EXECUTE=true` – let 'er loose
- [ ] `POLYGON_PRIVATE_KEY` – she needs yer private key to get intimate with the blockchain
- [ ] `WALLET_ADDRESS` – so she knows where home is
- [ ] Got USDC loaded up on that Polygon wallet – can't ride without gas money
- [ ] Optional: `ALCHEMY_API_KEY` fer them premium RPCs (she deserves the good stuff)
- [ ] Optional: `FULL_REDEEM_ENABLED=true` – auto-collect winnings
- [ ] Open up `/viz` dashboard an' give 'er a once-over

## 🔍 What This Frisky Lil' Bot Does

1. **5min Pre-Filter**: Eyeballs ~2395 markets, narrows it down to 15-30 juicy 5min BTC/ETH/SOL opportunities
2. **EdgeEngine v2**: Figures out where the sweet spot is usin' volatility + Z-Score – she's real analytical-like
3. **EV-Check**: Won't spread 'er legs fer just any trade – needs EV > MIN_EV or she ain't interested
4. **Kelly Sizing**: Half-Kelly (0.5) – paces herself so she don't blow 'er whole wad at once
5. **Auto-Execute**: When AUTO_EXECUTE=true an' DRY_RUN=false – she goes to town, no chaperone needed
6. **Full Redeem V89**: Scans on-chain for ALL redeemable positions – never forgets yer winnings!

## 📊 What She's Packin'

| Feature | Status | The Dirty Details |
|---------|--------|-------------------|
| 5-Min Pre-Filter | ✅ | 2395 markets → ~20 worth gettin' into |
| EdgeEngine v2 | ✅ | Volatility + Z-Score lovin' |
| Backtester + Report | ✅ | Winrate, Sharpe, PNL – she keeps score |
| Optuna Hyperopt | ✅ | TPE Bayesian Optimization – fancy foreplay |
| Auto-Trade + Half-Kelly | ✅ | Goes all the way when DRY_RUN=false |
| Full Redeemer V89 | ✅ | On-chain scan – never loses yer winnings |
| Internal Dashboard | ✅ | /viz + /monitor – peep show included |
| Graceful Shutdown | ✅ | Knows when to pull out if thangs go south |
| Detailed Rejection Logging | ✅ | Tells ya exactly why she said no |

## 🌐 Where to Watch 'Er Perform

- `/viz` – Optuna plots an' strategy comparison (the money shot)
- `/monitor` – Real-time PNL chart an' trades (watch 'er work it)
- `/api/health` – Quick check she ain't passed out
- `/api/status` – Full rundown of what she's up to
- `/api/force_full_redeem` – POST to cash out all winning positions
- `/api/full_redeemer_status` – GET to check full redeemer status

## ⚠️ Words of Wisdom from Yer Uncle Jeb

1. **This bot plays with REAL money** when `DRY_RUN=false` – she ain't fakin' it
2. **Keep yer private key locked up tighter'n a bull's backside in fly season** – don't you dare commit it!
3. **Check them logs** – Railway dashboard or `/api/status` will tell ya if she's been naughty
4. **She needs liquidity** – won't touch a market with less'n $200 sloshin' around

## 🛠️ Tinkerin' in the Barn (Local Dev)

```bash
# Git 'er installed
pip install -e ".[dev]"

# Run them tests – make sure she's in workin' order
python -m pytest tests/ -v

# Take 'er fer a test ride (DRY_RUN=true so she don't spend yer money!)
DRY_RUN=true uvicorn polybot.main_fastapi:app --reload
```

## 📝 How She Got Here

- `production-ready`: She finally puts out – real trades, strong 5min filter, proper defaults
- `V89 Full Redeemer`: Independent on-chain position scanner – never forgets yer winnings!

## 🔄 Migrate to polybot2 (Fresh Clone)

If `git clone` is givin' ya trouble (big history, corrupted objects, etc.), you can
start fresh with a brand-new repo called **polybot2** that has all the current files
but none of the old git baggage:

```bash
# From inside the PolyBot directory (or any checkout):
./migrate_to_polybot2.sh            # creates ../polybot2

# Then push to GitHub:
cd ../polybot2
gh repo create polybot2 --private --source=. --push
```

The original PolyBot directory stays untouched.

---

**May yer positions be long, yer drawdowns be short, an' may she never go down on ya unexpectedly!** 🤠💰🍑
