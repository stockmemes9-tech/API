"""
CoinSwitch PRO Futures — "Resistance Short" scanner/bot
=========================================================

Strategy (as described by the user):
  Short a coin when ALL of the following are true:
    1. It is NOT in the top-100 cryptos by global market cap.
    2. It is down more than 5% in the last 24 hours.
    3. It has more than 10 crore (10,00,00,000) INR of trading volume in the last 24 hours.
    4. On the 5-minute chart, price is currently sitting at a resistance level.

This script:
    - Pulls the global top-100 market-cap list from CoinGecko (free, no key needed)
      and excludes those symbols.
    - Pulls 24h stats for every CoinSwitch futures symbol in one call.
    - Filters by % drop and INR volume (converted from the USDT volume CoinSwitch
      reports, using a live USDT/INR rate from CoinGecko).
    - For each surviving symbol, pulls 5m candles and looks for swing-high
      ("pivot high") resistance levels, then checks whether the current price
      is sitting just under one of them (optionally requiring a rejection wick).
    - If everything matches, places a MARKET short (no stop-loss order) and a
      take-profit limit order, and sends a Telegram alert for both.

IMPORTANT — read before running
--------------------------------
    - DRY_RUN defaults to True. It will only print what it *would* do. Do not
      flip it to False until you've watched it run in dry mode for a while and
      are comfortable with what it's selecting.
    - This is a heuristic resistance detector (swing-high clustering), not a
      guarantee of an actual resistance level. False positives will happen,
      especially in choppy/low-liquidity charts. Always sanity-check the
      instrument list it produces.
    - "Not in top 100 by market cap" is matched by ticker symbol against
      CoinGecko's top 100. Ticker symbols can collide across unrelated coins
      (e.g. multiple projects called "SUN"), so double check the actual name
      of anything it's about to short, not just the symbol.
    - Shorting futures uses leverage: losses can exceed your margin quickly,
      especially on low-cap/low-liquidity coins with wide spreads and violent
      wicks. Position sizing and leverage below are placeholders — set them
      deliberately, not by copy-pasting.
    - I'm not a financial advisor and this isn't financial advice — this is a
      technical implementation of the rules you described. Please validate the
      logic against your own judgment before risking real capital.

Setup
-----
    pip install requests cryptography --break-system-packages   (if on Linux w/ externally managed env)
    pip install requests cryptography                            (Windows / normal venv)

Config is read from environment variables (see CONFIG section below for the
exact names). For local runs, either export them in your shell or create a
`.env` file and load it (not included here to avoid adding a dependency) —
or just temporarily hardcode values while testing locally, but don't commit
them. For Railway deployment, set them under Project -> Variables instead.

Run:
    python coinswitch_resistance_short_bot.py
"""

import os
import time
import urllib.parse
import requests
from cryptography.hazmat.primitives.asymmetric import ed25519


def require_env(name):
    """Fetch a required env var, or fail fast with a clear message instead of
    a confusing KeyError/None deep inside a request later."""
    value = os.environ.get(name)
    if not value:
        raise SystemExit(
            f"Missing required environment variable: {name}. "
            f"Set it in your shell (local run) or in Railway -> Variables (deployed run)."
        )
    return value


# ============================== CONFIG ======================================

API_KEY = require_env("COINSWITCH_API_KEY")
SECRET_KEY = require_env("COINSWITCH_SECRET_KEY")

BASE_URL = "https://coinswitch.co"
EXCHANGE = "EXCHANGE_2"  # CoinSwitch futures exchange identifier

# DRY_RUN reads from env too, defaulting to True (safe) if not set at all.
# Set DRY_RUN=false in Railway variables only once you trust what it's picking.
DRY_RUN = os.environ.get("DRY_RUN", "true").strip().lower() not in ("false", "0", "no")

# --- Screener thresholds ---
TOP_N_MARKET_CAP_EXCLUDE = 100
MIN_24H_DROP_PCT = 5.0                # price must be down at least this much
MIN_24H_VOLUME_INR = 10_00_00_000     # 10 crore INR

# --- Resistance detection (5m chart) ---
KLINE_INTERVAL = "5"                  # minutes; "5" = 5m candles
RESISTANCE_LOOKBACK_CANDLES = 150     # ~12.5 hours of 5m candles
PIVOT_WING = 3                        # candles on each side to confirm a swing high
RESISTANCE_TOLERANCE_PCT = 0.4        # "at resistance" = within this % of a swing-high cluster
REQUIRE_REJECTION_CANDLE = True       # also require the latest candle to show a rejection wick

# --- Order sizing / risk ---
CAPITAL_INR = 15_000                  # fixed margin per trade, in INR (converted to USDT at runtime)
LEVERAGE = min(5, 5)                  # hard-capped at 5x, even if someone edits the first "5" above by mistake
# NOTE: no stop-loss order is placed by this script. Shorts run without a hard
# exit unless the take-profit fills. On leveraged futures that means an
# adverse move can draw down your margin with nothing automatically closing
# the position - you are relying entirely on manual monitoring / Telegram
# alerts below to intervene. This was a deliberate choice at your request.

# Take-profit is expressed as a % return on CAPITAL, not on the leveraged notional.
# At 5x leverage, a 5% return on capital only needs a 1% move in price
# (TP_CAPITAL_PCT / LEVERAGE) — the line below does that conversion for you
# automatically, so it stays correct even if you change LEVERAGE later.
TP_CAPITAL_PCT = 5.0                  # target: 5% profit on the 15k capital
TP_PRICE_PCT = TP_CAPITAL_PCT / LEVERAGE  # -> 1.0% price move at 5x leverage

MAX_CONCURRENT_SHORTS = 3             # simple in-memory cap on open positions this run
MAX_TRADES_PER_DAY = 7                # hard cap on new entries per calendar day (resets at midnight, local time)

POLL_INTERVAL_SECONDS = 300           # rescan cadence — matches the 5m chart

# --- Telegram notifications ---
# 1. Message @BotFather on Telegram, send /newbot, follow the prompts -> you get a bot token.
# 2. Start a chat with your new bot (search its username, send it any message).
# 3. Visit https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates in a browser after step 2
#    and find "chat":{"id": ...} in the JSON -- that's your TELEGRAM_CHAT_ID.
# Both read from env vars; notifications silently no-op if either is unset.
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
ENABLE_TELEGRAM_NOTIFICATIONS = os.environ.get("ENABLE_TELEGRAM_NOTIFICATIONS", "true").strip().lower() not in ("false", "0", "no")

# =============================================================================


# ------------------------- CoinSwitch auth helper ---------------------------
# From CoinSwitch's official Reference Client docs.
def sign_request(method, path, params=None):
    method = method.upper()
    if params:
        sep = "&" if "?" in path else "?"
        path = path + sep + urllib.parse.urlencode(params)
    decoded_path = urllib.parse.unquote_plus(path)

    epoch = str(int(time.time() * 1000))
    message = method + decoded_path + epoch

    secret = ed25519.Ed25519PrivateKey.from_private_bytes(bytes.fromhex(SECRET_KEY))
    signature = secret.sign(message.encode("utf-8")).hex()

    headers = {
        "Content-Type": "application/json",
        "X-AUTH-APIKEY": API_KEY,
        "X-AUTH-SIGNATURE": signature,
        "X-AUTH-EPOCH": epoch,
    }
    return headers, decoded_path


# ------------------------------ Telegram --------------------------------------

def send_telegram_message(text):
    """Best-effort Telegram alert. Never lets a notification failure crash a trade cycle."""
    if not ENABLE_TELEGRAM_NOTIFICATIONS:
        return
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("  [telegram] TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set, skipping alert.")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
    except Exception as e:
        print(f"  [telegram] failed to send alert: {e}")


# ------------------------------ Market cap -----------------------------------

def get_top_market_cap_symbols(n=100):
    """Returns a set of uppercase ticker symbols in the global top-n by market cap."""
    resp = requests.get(
        "https://api.coingecko.com/api/v3/coins/markets",
        params={"vs_currency": "usd", "order": "market_cap_desc", "per_page": n, "page": 1},
        timeout=15,
    )
    resp.raise_for_status()
    return {c["symbol"].upper() for c in resp.json()}


def get_usdt_inr_rate():
    resp = requests.get(
        "https://api.coingecko.com/api/v3/simple/price",
        params={"ids": "tether", "vs_currencies": "inr"},
        timeout=15,
    )
    resp.raise_for_status()
    return float(resp.json()["tether"]["inr"])


# ------------------------------ CoinSwitch data -------------------------------

def get_all_tickers():
    headers, path = sign_request(
        "GET", "/trade/api/v2/futures/all-pairs/ticker", {"exchange": EXCHANGE}
    )
    r = requests.get(BASE_URL + path, headers=headers, timeout=15)
    r.raise_for_status()
    return r.json()["data"]


def get_klines(symbol, interval=KLINE_INTERVAL, limit=RESISTANCE_LOOKBACK_CANDLES,
                max_retries=3, retry_delay_seconds=2.0):
    headers, path = sign_request(
        "GET",
        "/trade/api/v2/futures/klines",
        {"symbol": symbol, "exchange": EXCHANGE, "interval": interval, "limit": limit},
    )
    for attempt in range(max_retries + 1):
        r = requests.get(BASE_URL + path, headers=headers, timeout=15)
        if r.status_code == 429:
            if attempt < max_retries:
                wait = retry_delay_seconds * (2 ** attempt)  # simple exponential backoff
                time.sleep(wait)
                continue
        r.raise_for_status()
        data = r.json()["data"]
        # klines come back most-recent-last per the docs' example; sort defensively by start_time
        return sorted(data, key=lambda c: int(c["start_time"]))
    r.raise_for_status()  # exhausted retries, surface the last error


def get_instrument_info():
    headers, path = sign_request(
        "GET", "/trade/api/v2/futures/instrument_info", {"exchange": EXCHANGE}
    )
    r = requests.get(BASE_URL + path, headers=headers, timeout=15)
    r.raise_for_status()
    return r.json()["data"]


def place_order(symbol, side, order_type, quantity, price=None,
                 trigger_price=None, reduce_only=False):
    body = {
        "exchange": EXCHANGE,
        "symbol": symbol,
        "side": side,
        "order_type": order_type,
        "quantity": quantity,
        "reduce_only": reduce_only,
    }
    if price is not None:
        body["price"] = price
    if trigger_price is not None:
        body["trigger_price"] = trigger_price

    if DRY_RUN:
        print(f"    [DRY RUN] would POST /futures/order -> {body}")
        return {"data": {"order_id": "DRY-RUN", "status": "DRY_RUN"}}

    headers, path = sign_request("POST", "/trade/api/v2/futures/order")
    r = requests.post(BASE_URL + path, headers=headers, json=body, timeout=15)
    r.raise_for_status()
    return r.json()


# ------------------------------ Screener step ---------------------------------

def screen_candidates(tickers, top_cap_symbols, usdt_inr_rate):
    """Apply rules 1-3: not top-100 cap, down >5% in 24h, >10cr INR volume."""
    candidates = []
    min_volume_usdt = MIN_24H_VOLUME_INR / usdt_inr_rate

    for symbol, t in tickers.items():
        base_symbol = symbol.replace("USDT", "").upper()
        if base_symbol in top_cap_symbols:
            continue

        try:
            # price_24h_pcnt is already a percentage, e.g. "-1.297300" means -1.2973%
            # (per CoinSwitch's docs example). Print a few raw values the first time you
            # run this if live numbers ever look off by a factor of 100.
            pct_change = float(t["price_24h_pcnt"])
        except (KeyError, ValueError):
            continue

        try:
            quote_volume = float(t["quote_asset_volume_24h"])
        except (KeyError, ValueError):
            continue

        if pct_change <= -MIN_24H_DROP_PCT and quote_volume >= min_volume_usdt:
            candidates.append({
                "symbol": symbol,
                "last_price": float(t["last_price"]),
                "pct_change_24h": pct_change,
                "quote_volume_24h_usdt": quote_volume,
            })

    return candidates


# ------------------------------ Resistance detection ---------------------------

def find_resistance_levels(candles, pivot_wing=PIVOT_WING, tolerance_pct=RESISTANCE_TOLERANCE_PCT):
    highs = [float(c["h"]) for c in candles]
    pivots = []
    for i in range(pivot_wing, len(highs) - pivot_wing):
        window = highs[i - pivot_wing: i + pivot_wing + 1]
        if highs[i] == max(window):
            pivots.append(highs[i])

    levels = []
    for h in sorted(pivots, reverse=True):
        if not any(abs(h - lvl) / lvl * 100 <= tolerance_pct for lvl in levels):
            levels.append(h)
    return levels


def is_at_resistance(current_price, levels, tolerance_pct=RESISTANCE_TOLERANCE_PCT):
    for lvl in levels:
        if current_price <= lvl and (lvl - current_price) / lvl * 100 <= tolerance_pct:
            return lvl
    return None


def has_rejection_candle(candles):
    """Very simple rejection check on the most recent closed candle:
    a red candle with an upper wick at least as large as the body."""
    if len(candles) < 2:
        return False
    c = candles[-1]
    o, h, l, close = float(c["o"]), float(c["h"]), float(c["l"]), float(c["c"])
    body = abs(close - o)
    upper_wick = h - max(o, close)
    return close < o and upper_wick >= body


def evaluate_resistance(symbol, current_price):
    candles = get_klines(symbol)
    if len(candles) < (2 * PIVOT_WING + 5):
        return None
    levels = find_resistance_levels(candles)
    hit = is_at_resistance(current_price, levels)
    if hit is None:
        return None
    if REQUIRE_REJECTION_CANDLE and not has_rejection_candle(candles):
        return None
    return hit


# ------------------------------ Sizing -----------------------------------------

def round_step(value, step):
    if step <= 0:
        return value
    precision = max(0, len(str(step).split(".")[1]) if "." in str(step) else 0)
    return round(round(value / step) * step, precision)


def compute_quantity(price, margin_usdt, leverage, instrument):
    notional = margin_usdt * leverage
    raw_qty = notional / price
    step = float(instrument.get("base_quantity_step_size", instrument.get("lot_size", "0.001")))
    min_qty = float(instrument.get("min_base_quantity", step))
    qty = round_step(raw_qty, step)
    return max(qty, min_qty)


# ------------------------------ Main loop ---------------------------------------

def run_once(instruments, top_cap_symbols, usdt_inr_rate, open_shorts, daily_trade_tracker):
    tickers = get_all_tickers()
    candidates = screen_candidates(tickers, top_cap_symbols, usdt_inr_rate)

    # Fixed 15,000 INR margin per trade, converted to USDT at the live rate.
    order_margin_usdt = CAPITAL_INR / usdt_inr_rate

    # Reset the daily counter if the calendar day has rolled over.
    today = time.strftime("%Y-%m-%d")
    if daily_trade_tracker["date"] != today:
        daily_trade_tracker["date"] = today
        daily_trade_tracker["count"] = 0

    print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] "
          f"{len(candidates)} symbol(s) pass the market-cap/drop/volume filter. "
          f"Trades today: {daily_trade_tracker['count']}/{MAX_TRADES_PER_DAY}")

    for cand in candidates:
        symbol = cand["symbol"]
        if symbol in open_shorts:
            continue
        if daily_trade_tracker["count"] >= MAX_TRADES_PER_DAY:
            print("  Daily trade limit reached, no further entries until tomorrow.")
            break
        if len(open_shorts) >= MAX_CONCURRENT_SHORTS:
            print("  Max concurrent shorts reached, skipping further entries this cycle.")
            break

        time.sleep(0.5)  # small pacing delay between klines calls to avoid 429 rate limiting

        try:
            resistance = evaluate_resistance(symbol, cand["last_price"])
        except requests.HTTPError as e:
            print(f"  {symbol}: klines fetch failed ({e}), skipping.")
            continue

        # Strict rule: never short unless price is confirmed sitting at a real
        # 5m resistance level (and, if enabled, showing a rejection candle).
        # evaluate_resistance() returns None for anything short of that, so no
        # entry below this line ever fires without a resistance confirmation.
        if resistance is None:
            continue

        print(f"  >>> {symbol}: {cand['pct_change_24h']:.2f}% 24h, "
              f"vol {cand['quote_volume_24h_usdt']:.0f} USDT, "
              f"price {cand['last_price']} at resistance ~{resistance:.6g} "
              f"(resistance-confirmed{' + rejection candle' if REQUIRE_REJECTION_CANDLE else ''}) — SHORT signal")

        instrument = instruments.get(symbol)
        if instrument is None:
            print(f"      no instrument info for {symbol}, skipping order.")
            continue

        qty = compute_quantity(cand["last_price"], order_margin_usdt, LEVERAGE, instrument)
        price_precision = int(instrument.get("price_precision", 4))

        resp = place_order(symbol, side="SELL", order_type="MARKET", quantity=qty)
        print(f"      order response: {resp['data']}")
        open_shorts.add(symbol)
        daily_trade_tracker["count"] += 1

        entry_msg = (
            f"{'[DRY RUN] ' if DRY_RUN else ''}SHORT {symbol}\n"
            f"Entry: {cand['last_price']} (market)\n"
            f"Qty: {qty}\n"
            f"24h: {cand['pct_change_24h']:.2f}%  |  Resistance: ~{resistance:.6g}\n"
            f"No stop-loss set on this position."
        )
        send_telegram_message(entry_msg)

        # Take-profit: 5% return on the 15k capital -> TP_PRICE_PCT price move at 5x leverage.
        if TP_CAPITAL_PCT > 0:
            tp_price = round(cand["last_price"] * (1 - TP_PRICE_PCT / 100), price_precision)
            tp_resp = place_order(symbol, side="BUY", order_type="LIMIT",
                                   quantity=qty, price=tp_price, reduce_only=True)
            print(f"      take-profit @ {tp_price} "
                  f"({TP_PRICE_PCT:.2f}% price move -> {TP_CAPITAL_PCT:.1f}% on capital): {tp_resp['data']}")
            send_telegram_message(
                f"{'[DRY RUN] ' if DRY_RUN else ''}Take-profit set for {symbol} @ {tp_price} "
                f"({TP_PRICE_PCT:.2f}% price move -> {TP_CAPITAL_PCT:.1f}% on capital)"
            )


def main():
    print("Fetching top-100 market cap list and USDT/INR rate from CoinGecko...")
    top_cap_symbols = get_top_market_cap_symbols(TOP_N_MARKET_CAP_EXCLUDE)
    usdt_inr_rate = get_usdt_inr_rate()
    print(f"USDT/INR ~= {usdt_inr_rate}")

    print("Fetching CoinSwitch futures instrument info...")
    instruments = get_instrument_info()

    open_shorts = set()  # in-memory only — resets if the script restarts
    daily_trade_tracker = {"date": time.strftime("%Y-%m-%d"), "count": 0}  # resets at midnight

    print(f"DRY_RUN = {DRY_RUN}. Max {MAX_TRADES_PER_DAY} trades/day. "
          f"Starting scan loop every {POLL_INTERVAL_SECONDS}s. Ctrl+C to stop.")
    send_telegram_message(
        f"{'[DRY RUN] ' if DRY_RUN else ''}Bot started. "
        f"Scanning every {POLL_INTERVAL_SECONDS}s, max {MAX_TRADES_PER_DAY} trades/day."
    )
    while True:
        try:
            run_once(instruments, top_cap_symbols, usdt_inr_rate, open_shorts, daily_trade_tracker)
        except requests.HTTPError as e:
            print(f"HTTP error this cycle: {e}")
            send_telegram_message(f"⚠️ HTTP error this cycle: {e}")
        except Exception as e:
            print(f"Unexpected error this cycle: {e}")
            send_telegram_message(f"⚠️ Unexpected error this cycle: {e}")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
