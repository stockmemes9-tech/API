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
import json
import datetime
import threading
import urllib.parse
import requests
from cryptography.hazmat.primitives.asymmetric import ed25519

# Guards every read-modify-write access to open_shorts / daily_trade_tracker.
# Two threads touch that shared state now: main()'s own 5-minute scan loop,
# and telegram_polling_loop() (a separate daemon thread) reacting instantly
# to a "Close" button tap in Telegram. Without this lock the two could
# interleave mid-update (e.g. a manual close landing in the middle of
# reconcile_open_shorts()'s own close-detection) and corrupt open_shorts or
# double-count a closed trade.
state_lock = threading.Lock()

# All "day" boundaries (daily trade cap, daily P&L summary) are computed in
# IST, since that's the trader's timezone — Railway's container clock is UTC
# and we don't want the day to roll over at 5:30am IST.
IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))


def today_ist():
    return datetime.datetime.now(IST).strftime("%Y-%m-%d")


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


def fetch_with_retry(func, *args, description="", max_wait=60, **kwargs):
    """Retries a one-time startup call indefinitely with capped exponential
    backoff, instead of letting a transient network blip when the container
    boots kill the whole process before the scan loop even starts. The main
    scan loop already survives per-cycle errors on its own (see main()'s
    while-loop) — this covers the gap before that loop begins."""
    delay = 5
    while True:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            label = description or getattr(func, "__name__", "startup call")
            print(f"[startup] {label} failed ({e}), retrying in {delay}s...")
            time.sleep(delay)
            delay = min(delay * 2, max_wait)


# ============================== CONFIG ======================================

API_KEY = require_env("COINSWITCH_API_KEY")
SECRET_KEY = require_env("COINSWITCH_SECRET_KEY")

BASE_URL = "https://coinswitch.co"
EXCHANGE = "EXCHANGE_2"  # CoinSwitch futures exchange identifier — crypto perpetuals only.
# This script only ever calls CoinSwitch's crypto futures endpoints (BASE_URL +
# /trade/api/v2/futures/*) under EXCHANGE_2. CoinSwitch doesn't offer US
# equities at all, so there's no code path here that could ever touch a stock —
# this line is the single hardcoded venue for every order the bot places.

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
DESIRED_LEVERAGE = 5                  # target leverage; if a symbol's max_leverage is lower,
                                       # resolve_leverage() falls back to that symbol's highest
                                       # available leverage instead of failing the order.
# NOTE: no stop-loss order is placed by this script. Shorts run without a hard
# exit unless the take-profit fills. On leveraged futures that means an
# adverse move can draw down your margin with nothing automatically closing
# the position - you are relying entirely on manual monitoring / Telegram
# alerts below to intervene. This was a deliberate choice at your request.

# Take-profit is expressed as a % return on CAPITAL, not on the leveraged notional.
# The actual price-move % needed depends on the leverage used for that specific
# trade (see resolve_leverage() — it can be less than DESIRED_LEVERAGE), so it's
# computed per-trade in run_once() rather than as a single constant here.
TP_CAPITAL_PCT = 5.0                  # target: 5% profit on the 15k capital

MAX_TRADES_PER_DAY = 10               # hard cap on new entries per calendar day (resets at midnight IST)
                                       # No cap on concurrent open positions — the bot will keep as many
                                       # open at once as the daily trade count and available wallet
                                       # balance allow.

POLL_INTERVAL_SECONDS = 300           # rescan cadence — matches the 5m chart

# --- Position monitoring ---
STATUS_UPDATE_INTERVAL_SECONDS = 15 * 60   # send an open-positions P&L snapshot to Telegram this often
LIQUIDATION_WARNING_PCT = 50.0             # alert once a position's adverse move has covered this
                                            # % of the distance from entry to its estimated liquidation
                                            # price (see estimate_liquidation_price() for the caveats
                                            # on how that estimate is derived).

# --- Telegram notifications ---
# 1. Message @BotFather on Telegram, send /newbot, follow the prompts -> you get a bot token.
# 2. Start a chat with your new bot (search its username, send it any message).
# 3. Visit https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates in a browser after step 2
#    and find "chat":{"id": ...} in the JSON -- that's your TELEGRAM_CHAT_ID.
# Both read from env vars; notifications silently no-op if either is unset.
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
ENABLE_TELEGRAM_NOTIFICATIONS = os.environ.get("ENABLE_TELEGRAM_NOTIFICATIONS", "true").strip().lower() not in ("false", "0", "no")

# --- Local state persistence ---
# Restores in-memory bookkeeping (open_shorts + today's daily_trade_tracker
# counters) across a restart. This is separate from recover_open_positions()
# below, which re-derives *actual* open positions from CoinSwitch itself —
# the exchange is always the source of truth for what's really open. What
# the exchange can NOT tell us on restart is today's trade count / win-loss /
# realized P&L so far (needed for MAX_TRADES_PER_DAY tracking and the daily
# summary), or a DRY_RUN (simulated) short's take-profit price and true
# entry time, since simulated trades never touched the real exchange at all.
# This file exists purely to carry that bookkeeping across a restart; it is
# never treated as authoritative for "is this symbol actually short right
# now" on a real position — the live exchange check always wins for that.
# On Railway without a mounted volume this path is ephemeral across
# redeploys (fine — recover_open_positions() still works from the exchange
# alone in that case, same as before this existed), but it survives a plain
# process crash/restart within the same deployment.
STATE_FILE_PATH = os.environ.get("STATE_FILE_PATH", "bot_state.json")

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

def send_telegram_message(text, reply_markup=None):
    """Best-effort Telegram alert. Never lets a notification failure crash a trade cycle.

    reply_markup, if given, is a Telegram InlineKeyboardMarkup dict, e.g.
    {"inline_keyboard": [[{"text": "...", "callback_data": "..."}]]} — used
    to attach the per-position "Close" buttons to status updates."""
    if not ENABLE_TELEGRAM_NOTIFICATIONS:
        return
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("  [telegram] TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set, skipping alert.")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
        if reply_markup is not None:
            payload["reply_markup"] = json.dumps(reply_markup)
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"  [telegram] failed to send alert: {e}")


def get_telegram_updates(offset=None, timeout=25):
    """Long-polls Telegram's getUpdates endpoint for new messages/button taps.
    Blocks up to ~timeout seconds server-side if there's nothing new yet —
    that's what lets telegram_polling_loop() react to a "Close" tap within
    a second or two instead of waiting for the next scan cycle."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    params = {"timeout": timeout}
    if offset is not None:
        params["offset"] = offset
    r = requests.get(url, params=params, timeout=timeout + 10)
    r.raise_for_status()
    return r.json().get("result", [])


def answer_callback_query(callback_query_id, text=""):
    """Acknowledges a button tap so Telegram stops showing the little loading
    spinner on it. Best-effort — a failure here shouldn't block the close."""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
        requests.post(url, json={"callback_query_id": callback_query_id, "text": text}, timeout=10)
    except Exception as e:
        print(f"  [telegram] failed to answer callback query: {e}")


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


def get_positions(symbol, max_retries=3, retry_delay_seconds=2.0):
    """Returns the list of currently OPEN positions for a symbol (empty list if
    none). Closed positions simply disappear from this endpoint — there's no
    terminal 'CLOSED' status to check for."""
    headers, path = sign_request(
        "GET", "/trade/api/v2/futures/positions", {"exchange": EXCHANGE, "symbol": symbol}
    )
    for attempt in range(max_retries + 1):
        r = requests.get(BASE_URL + path, headers=headers, timeout=15)
        if r.status_code == 429 and attempt < max_retries:
            time.sleep(retry_delay_seconds * (2 ** attempt))
            continue
        r.raise_for_status()
        body = r.json()
        if "data" not in body:
            # Confirmed from real CoinSwitch responses: when a symbol has no
            # open positions, this endpoint returns HTTP 200 with
            # {"message": "There are no open Positions"} instead of
            # {"data": []}. That's the expected, common-case response for
            # most symbols (most won't have a position open) — not an error —
            # so treat it as an empty position list rather than logging every
            # single no-position symbol as a "failure" during recovery.
            message = str(body.get("message", "")).lower()
            if "no open position" in message:
                return []
            # Anything else without a "data" field genuinely is unexpected —
            # print the raw body so it's diagnosable from logs instead of
            # surfacing as an opaque KeyError('data'), and raise something
            # the caller can catch alongside HTTPError so ONE bad symbol
            # doesn't abort the whole recovery scan.
            raise RuntimeError(
                f"CoinSwitch /positions response for {symbol} has no 'data' field and isn't "
                f"the known 'no open positions' message. HTTP {r.status_code}, raw body: {body}"
            )
        return body["data"]


def get_realized_pnl(symbol, from_time_ms):
    """Sums the realized P&L (USDT) recorded for a symbol since from_time_ms.
    Same caveat as get_positions: the exact 'amount' field name is unverified
    against CoinSwitch's live API. Individual bad/missing entries are skipped
    with a warning rather than raising and aborting the whole reconcile cycle."""
    headers, path = sign_request(
        "GET",
        "/trade/api/v2/futures/transactions",
        {"exchange": EXCHANGE, "symbol": symbol, "type": "P&L", "from_time": from_time_ms},
    )
    r = requests.get(BASE_URL + path, headers=headers, timeout=15)
    r.raise_for_status()
    total = 0.0
    for t in r.json()["data"]:
        try:
            total += float(t["amount"])
        except (KeyError, ValueError, TypeError):
            print(f"  [reconcile] {symbol}: transaction entry missing/bad 'amount' field, skipping it. Raw: {t}")
    return total


def get_wallet_balance():
    """Returns the available USDT futures wallet balance (float) — the amount
    free to use for new orders/margin, per CoinSwitch's Get Wallet Balance
    endpoint. Raises requests.HTTPError on failure (caller decides how to
    handle a transient lookup failure)."""
    headers, path = sign_request("GET", "/trade/api/v2/futures/wallet_balance")
    r = requests.get(BASE_URL + path, headers=headers, timeout=15)
    r.raise_for_status()
    base_asset_balances = r.json()["data"]["base_asset_balances"]
    for entry in base_asset_balances:
        if entry.get("base_asset") == "USDT":
            return float(entry["balances"]["total_available_balance"])
    # No USDT row at all — treat as zero available rather than raising, so a
    # single unexpected response shape doesn't crash the whole scan cycle.
    print(f"  [wallet] no USDT entry found in wallet balance response: {base_asset_balances}")
    return 0.0


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
    # format(..., 'f') avoids scientific notation (e.g. str(1e-05) == "1e-05",
    # which has no "." and silently produced precision=0 — rounding a tiny
    # step size's quantity down to a whole number instead of its real decimals).
    step_str = format(step, "f")
    precision = max(0, len(step_str.split(".")[1]) if "." in step_str else 0)
    return round(round(value / step) * step, precision)


def compute_quantity(price, margin_usdt, leverage, instrument):
    notional = margin_usdt * leverage
    raw_qty = notional / price
    step = float(instrument.get("base_quantity_step_size", instrument.get("lot_size", "0.001")))
    min_qty = float(instrument.get("min_base_quantity", step))
    qty = round_step(raw_qty, step)
    if qty < min_qty:
        # Bumping up to the exchange minimum silently increases the actual
        # notional beyond what CAPITAL_INR x leverage intended — flag it
        # loudly rather than let margin risk grow unnoticed.
        print(f"      [sizing] computed qty {raw_qty:.6g} is below this symbol's minimum "
              f"tradable size ({min_qty}); using {min_qty} instead — position will be larger "
              f"than the intended margin.")
        qty = min_qty
    return qty


def resolve_leverage(instrument, desired=DESIRED_LEVERAGE):
    """Use `desired`x if the symbol supports it; otherwise fall back to the
    highest leverage that symbol allows (never higher than desired, never
    below the symbol's own minimum)."""
    try:
        max_lev = float(instrument.get("max_leverage", desired))
    except (TypeError, ValueError):
        max_lev = desired
    try:
        min_lev = float(instrument.get("min_leverage", 1))
    except (TypeError, ValueError):
        min_lev = 1
    try:
        step = float(instrument.get("leverage_step") or 1)
    except (TypeError, ValueError):
        step = 1

    # NOTE: if a symbol's own min_leverage is above `desired`, this can return
    # MORE leverage than requested (forcing it back down to `desired` would
    # just make set_leverage() get rejected — you can't run below a symbol's
    # own floor). This is the opposite of the usual "fall back to what's
    # available" case, so the caller must check both directions, not just
    # "leverage < desired", or this ships with silently higher risk.
    effective = max(min(desired, max_lev), min_lev)
    if step > 0:
        # snap down to the nearest valid step at/above the symbol's minimum
        steps_above_min = int((effective - min_lev) / step + 1e-9)
        effective = min_lev + steps_above_min * step
    return int(effective) if effective == int(effective) else effective


def set_leverage(symbol, leverage):
    """Sets leverage for a symbol right before opening a fresh position.
    CoinSwitch rejects this call if there are open orders/positions on the
    symbol already, which is fine here since we only call it for symbols not
    already in open_shorts."""
    if DRY_RUN:
        print(f"    [DRY RUN] would set leverage to {leverage}x on {symbol}")
        return
    headers, path = sign_request("POST", "/trade/api/v2/futures/leverage")
    body = {"symbol": symbol, "exchange": EXCHANGE, "leverage": leverage}
    r = requests.post(BASE_URL + path, headers=headers, json=body, timeout=15)
    r.raise_for_status()
    return r.json()


# ------------------------------ State persistence --------------------------------

def save_state(open_shorts, daily_trade_tracker):
    """Best-effort local persistence of in-memory bookkeeping. Called right
    after every runtime mutation of open_shorts (new short opened, short
    closed during reconcile) so a crash/redeploy between cycles loses at
    most the last few seconds of state, not the whole day. Never raises —
    a failed write here should not take down a trade cycle; worst case a
    future restart falls back to the live-recovery-only behavior this bot
    already had before state persistence existed."""
    try:
        payload = {
            "open_shorts": open_shorts,
            "daily_trade_tracker": daily_trade_tracker,
            "saved_at_ms": int(time.time() * 1000),
        }
        tmp_path = STATE_FILE_PATH + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(payload, f)
        os.replace(tmp_path, STATE_FILE_PATH)  # atomic on POSIX — avoids a torn/partial
                                                # state file if the process dies mid-write
    except Exception as e:
        print(f"  [state] failed to save state file: {e}")


def load_state():
    """Returns (open_shorts, daily_trade_tracker) loaded from the local state
    file, or (None, None) if it doesn't exist or can't be parsed. A missing
    or corrupt file is NOT an error worth failing startup over — it just
    means recovery falls back to live-exchange-only reconstruction, same as
    before state persistence existed."""
    try:
        with open(STATE_FILE_PATH, "r") as f:
            payload = json.load(f)
        return payload.get("open_shorts") or {}, payload.get("daily_trade_tracker")
    except FileNotFoundError:
        return None, None
    except Exception as e:
        print(f"  [state] failed to load state file ({e}), starting without saved state.")
        return None, None


# ------------------------------ Startup recovery --------------------------------
#
# Runs once, before the scan loop starts. Without this, open_shorts always
# starts empty on a restart (crash, redeploy, manual stop) — meaning a real
# still-open position from before the restart would go untracked, and the
# bot could open a second position on the same symbol without realizing one
# already exists. The live exchange check below is READ-ONLY (no orders
# placed), so it's safe to run regardless of DRY_RUN.
#
# The exchange alone can't tell us everything though: today's trade count /
# win-loss / realized P&L (MAX_TRADES_PER_DAY + daily summary tracking), or
# a DRY_RUN short's take-profit price and true entry time, since simulated
# trades never touch the real exchange. That's what the local state file
# (saved via save_state() on every mutation) fills in — real open positions
# are still always re-derived from CoinSwitch itself as the source of truth.

def recover_open_positions(instruments, daily_trade_tracker):
    state_open_shorts, state_daily_tracker = load_state()
    state_open_shorts = state_open_shorts or {}

    # Restore today's counters if the saved state is from today (IST).
    # These can't be reconstructed from the exchange at all — without this,
    # every restart mid-day would silently reset MAX_TRADES_PER_DAY tracking
    # and the daily P&L stats sent in the end-of-day summary.
    if state_daily_tracker and state_daily_tracker.get("date") == today_ist():
        daily_trade_tracker.update(state_daily_tracker)
        print(f"  [state] restored today's counters from saved state: "
              f"{daily_trade_tracker['count']} trade(s) opened, "
              f"{daily_trade_tracker['trades_closed']} closed, "
              f"P&L so far {daily_trade_tracker['realized_pnl_usdt']:+.2f} USDT.")
    elif state_daily_tracker:
        print(f"  [state] saved state is from a previous day "
              f"({state_daily_tracker.get('date')}), not restoring today's counters.")

    # Simulated (DRY_RUN) shorts never touched the real exchange, so there's
    # nothing to verify them against — the saved state IS the only record of
    # them. Carry them over as-is.
    recovered = {
        symbol: pos for symbol, pos in state_open_shorts.items() if pos.get("simulated")
    }
    for pos in recovered.values():
        # Backfill defaults for keys that didn't exist in state files saved
        # before liquidation monitoring was added, so older saved state
        # doesn't crash check_liquidation_warnings()/send_position_status_update().
        pos.setdefault("leverage", DESIRED_LEVERAGE)
        pos.setdefault("liquidation_warning_sent", False)
    if recovered:
        print(f"  [state] restored {len(recovered)} simulated (DRY RUN) open short(s) "
              f"from saved state: {', '.join(recovered.keys())}")

    symbols = list(instruments.keys())
    print(f"Checking {len(symbols)} symbols on CoinSwitch for pre-existing open positions...")
    for i, symbol in enumerate(symbols):
        try:
            positions = get_positions(symbol)
        except Exception as e:
            # Deliberately broad, not just requests.HTTPError: a malformed
            # response (e.g. HTTP 200 with an unexpected body shape) used to
            # escape this try/except entirely and abort the whole startup
            # scan, which fetch_with_retry() would then restart from symbol
            # #1 — hitting the exact same failure forever and never actually
            # starting the bot. Skipping just this one symbol and continuing
            # is a much safer failure mode; SystemExit/KeyboardInterrupt
            # still propagate normally since neither is an Exception subclass.
            print(f"  [recover] {symbol}: position check failed ({e}), skipping. "
                  f"If this symbol genuinely has an open position, it won't be tracked "
                  f"until a future restart succeeds in checking it.")
            time.sleep(3.1)  # still pace this like a normal call, so a run of
                              # consecutive bad-response symbols can't 429-storm
                              # the API the way an un-paced tight loop would.
            continue

        if positions:
            # CoinSwitch's real /futures/positions schema (per official docs):
            # entry price -> "avg_entry_price", size -> "position_size",
            # direction -> "position_side" ("LONG"/"SHORT"). The old field-name
            # guessing here never matched those, so entry_price/qty were always
            # None. Also skip anything that isn't actually a SHORT — this bot
            # never opens longs, so a LONG on a symbol is either a manual
            # position or leftover from something else, and treating it as one
            # of ours would permanently block shorting that symbol and corrupt
            # P&L math whenever it's "closed".
            p = positions[0]
            if p.get("position_side") not in (None, "SHORT"):
                print(f"  [recover] {symbol}: open position is {p.get('position_side')}, "
                      f"not SHORT — not tracking it as one of this bot's trades. Raw: {p}")
                time.sleep(3.1)
                continue

            entry_price = None
            for key in ("avg_entry_price", "entry_price", "avg_price", "average_price"):
                try:
                    entry_price = float(p[key])
                    break
                except (KeyError, ValueError, TypeError):
                    continue
            qty = None
            for key in ("position_size", "quantity", "size", "position_amount", "qty"):
                try:
                    qty = float(p[key])
                    break
                except (KeyError, ValueError, TypeError):
                    continue

            # The exchange is always trusted over saved state for entry_price
            # and qty (it's more current), but it has no concept of "our"
            # take-profit order or the true opened_at_ms — backfill those
            # from saved state when this symbol matches a real (non-simulated)
            # entry recorded there.
            saved = state_open_shorts.get(symbol)
            if saved and not saved.get("simulated"):
                tp_price = saved.get("tp_price")
                opened_at_ms = saved.get("opened_at_ms", int(time.time() * 1000))
                liquidation_warning_sent = saved.get("liquidation_warning_sent", False)
            else:
                tp_price = None
                opened_at_ms = int(time.time() * 1000)  # true entry time unknown otherwise
                liquidation_warning_sent = False

            # Leverage actually set on the exchange for a position opened
            # before this restart isn't returned consistently by every
            # CoinSwitch response shape, so try the position payload first,
            # then fall back to whatever we had saved for this symbol, and
            # only then to DESIRED_LEVERAGE as a last resort. A wrong
            # fallback here only affects the liquidation-distance ESTIMATE
            # (see estimate_liquidation_price()) — it never changes what
            # order gets placed, since no new order is placed on recovery.
            leverage = None
            for key in ("leverage", "leverage_multiplier", "position_leverage"):
                try:
                    leverage = float(p[key])
                    break
                except (KeyError, ValueError, TypeError):
                    continue
            if leverage is None:
                leverage = (saved.get("leverage") if saved else None) or DESIRED_LEVERAGE
                print(f"      {symbol}: exchange didn't report leverage on this recovered "
                      f"position, using {leverage}x for the liquidation estimate (may be wrong "
                      f"if the real leverage set on this position differs).")

            recovered[symbol] = {
                "entry_price": entry_price,   # may be None if the field name didn't match — logged below either way
                "qty": qty,
                "tp_price": tp_price,
                "opened_at_ms": opened_at_ms,
                "simulated": False,           # always a real exchange position, regardless of today's DRY_RUN setting
                "leverage": leverage,
                "liquidation_warning_sent": liquidation_warning_sent,
            }
            print(f"  [recover] {symbol}: found an existing open position — now tracked. Raw: {p}")

        time.sleep(3.1)  # Get Positions is rate-limited to 20 req/60s per CoinSwitch's
                          # docs (~1 every 3s); the old 1s-per-10-calls pacing was
                          # 5-10x over that budget and would 429-storm on startup
                          # across a few hundred symbols.

    if recovered:
        print(f"Recovered {len(recovered)} open position(s) total (live + saved simulated): "
              f"{', '.join(recovered.keys())}")
        send_telegram_message(
            f"Startup: recovered {len(recovered)} open position(s) from CoinSwitch/saved state: "
            f"{', '.join(recovered.keys())}"
        )
    else:
        print("  [recover] no pre-existing open positions found (live or saved).")

    save_state(recovered, daily_trade_tracker)  # persist the merged result immediately,
                                                 # so a second restart before any trade
                                                 # activity still has a consistent file.
    return recovered


# ------------------------------ Position reconciliation --------------------------
#
# THE BUG THAT CAUSED THE FREEZE (historical — back when there was a
# concurrent-position cap): the old version only ever added symbols to
# open_shorts (in place_order's call site) and never removed them, so once
# the cap's worth of entries had fired, every future cycle hit "Max
# concurrent shorts reached" forever — even though nothing was actually still
# open. This function is what's missing: on each cycle, check whether every
# tracked short has actually closed, and if so, drop it from open_shorts and
# fold its P&L into the daily tracker.

def reconcile_open_shorts(open_shorts, tickers, daily_trade_tracker):
    closed = []
    for symbol, pos in list(open_shorts.items()):
        # Per-position flag, not the global DRY_RUN — a position recovered
        # from the real account at startup (or opened while DRY_RUN was
        # previously false) is always real and must be closed-checked
        # against the live API, even if the bot is running in DRY_RUN today.
        is_simulated = pos.get("simulated", DRY_RUN)

        if is_simulated:
            # No real order was placed, so there's no real position to poll.
            # We simulate the only exit this bot ever places — the take-profit
            # limit — by checking whether the live price has reached it.
            # (No stop-loss is set, by design, so this is the sole exit we
            # can simulate; a DRY RUN short can otherwise stay open forever.)
            t = tickers.get(symbol)
            if t is None:
                continue
            try:
                last_price = float(t["last_price"])
            except (KeyError, ValueError):
                continue
            if pos["tp_price"] is not None and last_price <= pos["tp_price"]:
                pnl = (pos["entry_price"] - pos["tp_price"]) * pos["qty"]
                closed.append((symbol, pnl))
        else:
            try:
                live_positions = get_positions(symbol)
            except Exception as e:
                # Broad on purpose, same reasoning as recover_open_positions():
                # a non-HTTPError failure here (e.g. a malformed 200 response)
                # used to escape uncaught and abort reconciliation for every
                # OTHER open symbol this cycle too, since this sits inside a
                # for-loop with only a per-iteration try/except around it.
                print(f"  [reconcile] {symbol}: position check failed ({e}), leaving tracked as open.")
                continue

            # IMPORTANT: the exact response schema for "closed" here is
            # unverified against CoinSwitch's live API (their docs site
            # wouldn't render for me while building this). An empty list is
            # the one signal we can trust — CoinSwitch's docs describe this
            # endpoint as returning currently-open positions, so nothing
            # returned for the symbol should mean nothing is open. If the
            # list is non-empty, we do NOT assume it's closed just because a
            # "status" field looks unfamiliar — better to leave a real
            # position tracked than to silently drop tracking of something
            # still open. Watch the
            # first day of live logs closely and confirm this behaves as
            # expected before trusting it unattended.
            if len(live_positions) > 0:
                unrecognized = [p for p in live_positions if p.get("status") not in ("OPEN", None)]
                if unrecognized:
                    print(f"  [reconcile] {symbol}: still has {len(live_positions)} position(s) "
                          f"reported, some with unrecognized status fields — leaving tracked as open. "
                          f"Raw: {live_positions}")
                continue

            try:
                pnl = get_realized_pnl(symbol, pos["opened_at_ms"])
            except requests.HTTPError as e:
                print(f"  [reconcile] {symbol}: P&L lookup failed ({e}), closing with unknown P&L.")
                pnl = 0.0
            closed.append((symbol, pnl))

    for symbol, pnl in closed:
        del open_shorts[symbol]
        daily_trade_tracker["realized_pnl_usdt"] += pnl
        daily_trade_tracker["trades_closed"] += 1
        if pnl >= 0:
            daily_trade_tracker["wins"] += 1
        else:
            daily_trade_tracker["losses"] += 1
        print(f"  [reconcile] {symbol}: position closed. P&L {pnl:+.2f} USDT.")
        send_telegram_message(
            f"{'[DRY RUN] ' if DRY_RUN else ''}{symbol} position closed. P&L: {pnl:+.2f} USDT"
        )

    if closed:
        save_state(open_shorts, daily_trade_tracker)


# ------------------------------ Position monitoring ------------------------------

def estimate_liquidation_price(entry_price, leverage):
    """Rough isolated-margin liquidation price estimate for a SHORT position,
    ignoring maintenance margin rate, funding, and fees — none of which this
    script fetches from CoinSwitch. A short's margin (entry_price*qty/leverage)
    is fully wiped once price has risen by entry_price/leverage, so:

        liq_price ~= entry_price * (1 + 1/leverage)

    In reality the exchange liquidates earlier than this once losses eat into
    the maintenance margin buffer, so treat this as an optimistic upper bound
    on how much room the position actually has — the real liquidation price
    is always somewhat below (i.e. closer, in adverse-move terms) this
    estimate. Good enough for an early-warning Telegram alert; not something
    to rely on for precise risk sizing."""
    if not leverage or leverage <= 0:
        return None
    return entry_price * (1 + 1.0 / leverage)


def check_liquidation_warnings(open_shorts, tickers):
    """Sends a one-time Telegram alert per position the first cycle its
    adverse move covers LIQUIDATION_WARNING_PCT of the distance from entry to
    its estimated liquidation price (see estimate_liquidation_price() for the
    caveats on that estimate). Re-arms itself (resets the flag) if price
    later moves back below the threshold, so a position that pokes across the
    line, retreats, and crosses again later gets alerted both times rather
    than going silent for the rest of its life."""
    changed = False
    for symbol, pos in open_shorts.items():
        entry_price = pos.get("entry_price")
        leverage = pos.get("leverage")
        if entry_price is None or leverage is None:
            continue  # can't estimate without both — e.g. a recovered position with an unmatched entry_price field

        t = tickers.get(symbol)
        if t is None:
            continue
        try:
            current_price = float(t["last_price"])
        except (KeyError, ValueError):
            continue

        liq_price = estimate_liquidation_price(entry_price, leverage)
        if liq_price is None or liq_price <= entry_price:
            continue
        distance_covered_pct = (current_price - entry_price) / (liq_price - entry_price) * 100

        already_sent = pos.get("liquidation_warning_sent", False)
        if distance_covered_pct >= LIQUIDATION_WARNING_PCT:
            if not already_sent:
                print(f"  [liquidation] {symbol}: {distance_covered_pct:.0f}% of the way to "
                      f"estimated liquidation ({liq_price:.6g}) — sending warning.")
                send_telegram_message(
                    f"⚠️ {'[DRY RUN] ' if pos.get('simulated') else ''}{symbol} short is "
                    f"~{distance_covered_pct:.0f}% of the way to its estimated liquidation price.\n"
                    f"Entry: {entry_price}  |  Current: {current_price}  |  Leverage: {leverage}x\n"
                    f"Est. liquidation: ~{liq_price:.6g} (rough estimate — ignores maintenance "
                    f"margin, so the real liquidation price is likely somewhat closer than this)."
                )
                pos["liquidation_warning_sent"] = True
                changed = True
        elif already_sent:
            # Price recovered back below the threshold — re-arm so a future
            # crossing alerts again instead of staying permanently silenced.
            pos["liquidation_warning_sent"] = False
            changed = True
    return changed


def send_position_status_update(open_shorts, tickers, force_send=False):
    """Periodic (STATUS_UPDATE_INTERVAL_SECONDS) Telegram snapshot of every
    open position's current unrealized P&L, plus the free wallet balance and
    one "❌ Close" button per open position — tapping it closes that position
    immediately via telegram_polling_loop(), without waiting for the next
    scan cycle. Skips sending entirely when nothing is open UNLESS
    force_send is True (used by the on-demand /status command, which should
    still reply with "no open positions" + wallet balance rather than go
    silent)."""
    if not open_shorts and not force_send:
        return

    lines = []
    total_unrealized = 0.0
    priced_count = 0
    keyboard_rows = []
    for symbol, pos in open_shorts.items():
        entry_price = pos.get("entry_price")
        qty = pos.get("qty")
        t = tickers.get(symbol)
        if entry_price is None or qty is None or t is None:
            lines.append(f"{symbol}: price/qty unavailable this cycle")
        else:
            try:
                current_price = float(t["last_price"])
            except (KeyError, ValueError):
                current_price = None
            if current_price is None:
                lines.append(f"{symbol}: current price unavailable this cycle")
            else:
                # SHORT: profit when price has fallen below entry.
                unrealized = (entry_price - current_price) * qty
                pct_move = (entry_price - current_price) / entry_price * 100
                emoji = "🟢" if unrealized > 0 else ("🔴" if unrealized < 0 else "⚪")
                total_unrealized += unrealized
                priced_count += 1
                lines.append(
                    f"{emoji} {symbol}{' [DRY RUN]' if pos.get('simulated') else ''}: "
                    f"{unrealized:+.2f} USDT ({pct_move:+.2f}% price move)  "
                    f"entry {entry_price} -> now {current_price}"
                )
        # One button per position regardless of whether it priced this cycle —
        # you should always be able to close a stuck/unpriced position too.
        keyboard_rows.append([{"text": f"❌ Close {symbol}", "callback_data": f"close:{symbol}"}])

    header_emoji = "🟢" if total_unrealized > 0 else ("🔴" if total_unrealized < 0 else "⚪")
    if open_shorts:
        msg = (
            f"{header_emoji} Open positions status ({priced_count}/{len(open_shorts)} priced)\n"
            + "\n".join(lines)
        )
        if priced_count:
            msg += f"\nTotal unrealized P&L: {total_unrealized:+.2f} USDT"
    else:
        msg = "No open positions right now."

    try:
        wallet_balance = get_wallet_balance()
        msg += f"\n\n💰 Wallet balance (free): {wallet_balance:.2f} USDT"
    except Exception as e:
        print(f"  [status update] wallet balance lookup failed: {e}")
        msg += "\n\n💰 Wallet balance: unavailable this cycle"

    print(f"\n[status update] {msg}")
    send_telegram_message(msg, reply_markup={"inline_keyboard": keyboard_rows} if keyboard_rows else None)


def send_daily_summary(daily_trade_tracker, open_shorts):
    pnl = daily_trade_tracker["realized_pnl_usdt"]
    emoji = "🟢" if pnl > 0 else ("🔴" if pnl < 0 else "⚪")
    msg = (
        f"{emoji} {'[DRY RUN] ' if DRY_RUN else ''}Daily summary — {daily_trade_tracker['date']}\n"
        f"Trades opened: {daily_trade_tracker['count']}\n"
        f"Trades closed: {daily_trade_tracker['trades_closed']} "
        f"(W {daily_trade_tracker['wins']} / L {daily_trade_tracker['losses']})\n"
        f"Realized P&L: {pnl:+.2f} USDT\n"
        f"Still open (carrying into today): {len(open_shorts)}"
    )
    print(f"\n[daily summary] {msg}")
    send_telegram_message(msg)


# ------------------------------ Manual close (Telegram button) -------------------

def close_position_manual(symbol, open_shorts, daily_trade_tracker):
    """Closes one open short immediately — triggered by tapping "❌ Close" under
    a Telegram status update. Caller (telegram_polling_loop) MUST already hold
    state_lock before calling this, since it reads-then-mutates open_shorts /
    daily_trade_tracker, the same state the 5-minute scan loop touches."""
    pos = open_shorts.get(symbol)
    if pos is None:
        send_telegram_message(f"⚠️ No open position found for {symbol} (already closed?).")
        return

    is_simulated = pos.get("simulated", DRY_RUN)
    qty = pos.get("qty")
    entry_price = pos.get("entry_price")

    if is_simulated:
        # Nothing real was ever placed on the exchange, so there's nothing to
        # send a close order for — just estimate P&L off the latest known
        # price so the daily tally stays meaningful, then drop it from tracking.
        last_price = None
        try:
            tickers = get_all_tickers()
            t = tickers.get(symbol)
            if t is not None:
                last_price = float(t["last_price"])
        except Exception as e:
            print(f"  [manual close] {symbol}: couldn't fetch price for P&L estimate ({e}).")
        if entry_price is not None and last_price is not None and qty is not None:
            pnl = (entry_price - last_price) * qty
        else:
            pnl = 0.0
        print(f"  [manual close] {symbol}: [DRY RUN] closing simulated position, est P&L {pnl:+.2f} USDT.")
    else:
        try:
            resp = place_order(symbol, side="BUY", order_type="MARKET", quantity=qty, reduce_only=True)
            print(f"  [manual close] {symbol}: close order placed -> {resp['data']}")
        except Exception as e:
            print(f"  [manual close] {symbol}: failed to place close order ({e}).")
            send_telegram_message(f"⚠️ Failed to close {symbol}: {e}")
            return

        # Give CoinSwitch a moment to settle the fill before asking for the
        # realized P&L, same as the market-order entry path does implicitly
        # via the next scan cycle — here we do it inline since this needs to
        # respond right away.
        time.sleep(2)
        try:
            pnl = get_realized_pnl(symbol, pos["opened_at_ms"])
        except Exception as e:
            print(f"  [manual close] {symbol}: P&L lookup failed ({e}), closing with unknown P&L.")
            pnl = 0.0

    del open_shorts[symbol]
    daily_trade_tracker["realized_pnl_usdt"] += pnl
    daily_trade_tracker["trades_closed"] += 1
    if pnl >= 0:
        daily_trade_tracker["wins"] += 1
    else:
        daily_trade_tracker["losses"] += 1
    save_state(open_shorts, daily_trade_tracker)

    send_telegram_message(
        f"✅ {'[DRY RUN] ' if is_simulated else ''}{symbol} manually closed. P&L: {pnl:+.2f} USDT"
    )


def send_on_demand_status(open_shorts, daily_trade_tracker):
    """Handles the /status command — an on-demand version of the periodic
    15-minute snapshot, sent immediately whenever you type /status in the
    chat rather than waiting for the timer. Caller MUST already hold
    state_lock, same as close_position_manual()."""
    try:
        tickers = get_all_tickers()
    except Exception as e:
        print(f"  [telegram] /status: failed to fetch tickers ({e}).")
        send_telegram_message(f"⚠️ Couldn't fetch current prices for /status: {e}")
        return
    send_position_status_update(open_shorts, tickers, force_send=True)


def telegram_polling_loop(open_shorts, daily_trade_tracker):
    """Runs for the lifetime of the process on its own daemon thread, separate
    from main()'s 5-minute scan loop — this is what lets tapping "❌ Close" in
    Telegram close a position within a second or two instead of waiting for
    the next scan cycle, and lets /status reply instantly too. Uses
    long-polling (getUpdates) rather than a webhook, since this bot doesn't
    run a web server to receive one.

    Every update's offset is advanced immediately, even for updates this loop
    doesn't act on, so Telegram never re-delivers the same tap/message forever."""
    if not ENABLE_TELEGRAM_NOTIFICATIONS or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("  [telegram] button/command polling disabled (notifications off or token/chat id not set).")
        return

    print("  [telegram] listening for 'Close' button taps and /status commands...")
    offset = None
    while True:
        try:
            updates = get_telegram_updates(offset)
        except Exception as e:
            print(f"  [telegram] getUpdates failed ({e}), retrying in 10s...")
            time.sleep(10)
            continue

        for update in updates:
            offset = update["update_id"] + 1  # advance regardless of whether we handle this update

            message = update.get("message")
            if message is not None:
                chat_id = str(message.get("chat", {}).get("id", ""))
                if chat_id != str(TELEGRAM_CHAT_ID):
                    continue  # single-user bot — ignore messages from any other chat
                # Strip a possible "@YourBotName" suffix (Telegram appends this
                # to commands in group chats) before matching.
                text = (message.get("text") or "").strip().split("@")[0].lower()
                if text == "/status":
                    print("  [telegram] /status requested")
                    with state_lock:
                        try:
                            send_on_demand_status(open_shorts, daily_trade_tracker)
                        except Exception as e:
                            print(f"  [telegram] /status failed unexpectedly: {e}")
                            send_telegram_message(f"⚠️ /status failed unexpectedly: {e}")
                continue

            cq = update.get("callback_query")
            if not cq:
                continue

            chat_id = str(cq.get("message", {}).get("chat", {}).get("id", ""))
            if chat_id != str(TELEGRAM_CHAT_ID):
                # This bot is single-user by design (it's sitting on your
                # exchange keys) — ignore taps from any other chat.
                answer_callback_query(cq.get("id", ""), "Not authorized.")
                continue

            data = cq.get("data", "")
            if not data.startswith("close:"):
                answer_callback_query(cq.get("id", ""))
                continue

            symbol = data[len("close:"):]
            answer_callback_query(cq.get("id", ""), f"Closing {symbol}...")
            print(f"  [telegram] 'Close' tapped for {symbol}")
            with state_lock:
                try:
                    close_position_manual(symbol, open_shorts, daily_trade_tracker)
                except Exception as e:
                    print(f"  [telegram] manual close of {symbol} failed unexpectedly: {e}")
                    send_telegram_message(f"⚠️ Closing {symbol} failed unexpectedly: {e}")


# ------------------------------ Main loop ---------------------------------------

def run_once(instruments, top_cap_symbols, usdt_inr_rate, open_shorts, daily_trade_tracker,
             last_market_refresh_date, last_status_update_ms):
    tickers = get_all_tickers()
    # Everything in this block reads and/or mutates open_shorts /
    # daily_trade_tracker, the same state telegram_polling_loop() touches the
    # instant a "Close" button is tapped — held under state_lock so a manual
    # close can't interleave mid-reconcile and corrupt the shared dicts.
    with state_lock:
        reconcile_open_shorts(open_shorts, tickers, daily_trade_tracker)

        # Liquidation-distance check runs every cycle (not on the 15-minute
        # status timer) since an adverse move can cross the warning threshold
        # well before the next scheduled status update.
        if check_liquidation_warnings(open_shorts, tickers):
            save_state(open_shorts, daily_trade_tracker)

        now_ms = int(time.time() * 1000)
        if now_ms - last_status_update_ms >= STATUS_UPDATE_INTERVAL_SECONDS * 1000:
            send_position_status_update(open_shorts, tickers)
            last_status_update_ms = now_ms

    today = today_ist()

    # Refresh the top-100 market-cap exclusion list and the USDT/INR
    # conversion rate once per IST calendar day. These were previously only
    # ever fetched once at process startup and then reused for the entire
    # lifetime of the container — on Railway that can mean running for days
    # against a market-cap ranking and FX rate that are stale by then. A coin
    # that's fallen out of (or risen into) the top 100 since startup would be
    # screened against the wrong exclusion list, and the margin-per-trade
    # sizing (CAPITAL_INR / usdt_inr_rate) would silently drift from its
    # intended INR value as the real USDT/INR rate moves.
    if last_market_refresh_date != today:
        try:
            top_cap_symbols = get_top_market_cap_symbols(TOP_N_MARKET_CAP_EXCLUDE)
            usdt_inr_rate = get_usdt_inr_rate()
            last_market_refresh_date = today
            print(f"  [refresh] top-100 market cap list and USDT/INR rate refreshed for {today} "
                  f"(USDT/INR ~= {usdt_inr_rate}).")
        except requests.HTTPError as e:
            # Don't let a transient CoinGecko blip abort this cycle's scan —
            # keep using the previous values and try the refresh again next
            # cycle (last_market_refresh_date is only advanced on success).
            print(f"  [refresh] failed to refresh market cap list / USDT-INR rate ({e}), "
                  f"keeping previous values for this cycle.")

    candidates = screen_candidates(tickers, top_cap_symbols, usdt_inr_rate)

    # Fixed 15,000 INR margin per trade, converted to USDT at the live rate.
    order_margin_usdt = CAPITAL_INR / usdt_inr_rate

    # Check available USDT balance before doing any real work this cycle.
    # If there isn't enough free margin for even one trade, there's no point
    # scanning/evaluating candidates at all this cycle — just wait for the
    # wallet to be topped up (or for a position to close and free margin)
    # and try again next cycle. This gate is skipped entirely when DRY_RUN
    # is on, so paper-trading can keep scanning/simulating regardless of the
    # real account balance. It's still enforced for live trading.
    try:
        available_balance_usdt = get_wallet_balance()
    except requests.HTTPError as e:
        print(f"  [wallet] balance check failed ({e}), skipping this cycle to be safe.")
        return top_cap_symbols, usdt_inr_rate, last_market_refresh_date, last_status_update_ms

    if not DRY_RUN and available_balance_usdt < order_margin_usdt:
        print(f"  [wallet] available balance {available_balance_usdt:.2f} USDT is below the "
              f"{order_margin_usdt:.2f} USDT needed for one trade — not searching for new trades "
              f"this cycle. Existing open positions are unaffected.")
        return top_cap_symbols, usdt_inr_rate, last_market_refresh_date, last_status_update_ms
    elif DRY_RUN and available_balance_usdt < order_margin_usdt:
        print(f"  [wallet] available balance {available_balance_usdt:.2f} USDT is below the "
              f"{order_margin_usdt:.2f} USDT needed for one trade — continuing to scan anyway "
              f"since DRY_RUN is on (no real orders will be placed).")

    # Reset the daily counters if the calendar day has rolled over (IST).
    # Send yesterday's P&L summary to Telegram before wiping the numbers.
    if daily_trade_tracker["date"] != today:
        with state_lock:
            send_daily_summary(daily_trade_tracker, open_shorts)
            daily_trade_tracker["date"] = today
            daily_trade_tracker["count"] = 0
            daily_trade_tracker["realized_pnl_usdt"] = 0.0
            daily_trade_tracker["trades_closed"] = 0
            daily_trade_tracker["wins"] = 0
            daily_trade_tracker["losses"] = 0
            save_state(open_shorts, daily_trade_tracker)

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
        # No cap on how many positions can be open at once — the only limits
        # are the daily trade count above and (in live trading) the wallet
        # balance below. available_balance_usdt is decremented locally (not
        # re-fetched) as each trade in this cycle consumes margin, so a burst
        # of candidates in one cycle can't collectively overdraw the wallet.
        # This check is skipped in DRY_RUN, so simulated runs aren't capped
        # by the real account balance.
        if not DRY_RUN and available_balance_usdt < order_margin_usdt:
            print(f"  [wallet] available balance {available_balance_usdt:.2f} USDT is now below "
                  f"the {order_margin_usdt:.2f} USDT needed for another trade — stopping new "
                  f"entries for this cycle.")
            break

        time.sleep(2.1)  # KLines is rate-limited to 30 req/60s per CoinSwitch's docs
                          # (~1 every 2s); 0.5s was ~4x over that budget and would
                          # 429-storm on scan cycles with several candidates.

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

        leverage = resolve_leverage(instrument)
        if leverage < DESIRED_LEVERAGE:
            print(f"      {symbol}: {DESIRED_LEVERAGE}x not available, using max {leverage}x instead.")
        elif leverage > DESIRED_LEVERAGE:
            print(f"      {symbol}: this symbol's own minimum leverage ({leverage}x) is above "
                  f"{DESIRED_LEVERAGE}x — trading at {leverage}x instead, which is MORE leverage "
                  f"than desired. Consider skipping this symbol if that's not acceptable.")
        set_leverage(symbol, leverage)

        qty = compute_quantity(cand["last_price"], order_margin_usdt, leverage, instrument)
        price_precision = int(instrument.get("price_precision", 4))

        resp = place_order(symbol, side="SELL", order_type="MARKET", quantity=qty)
        opened_at_ms = int(time.time() * 1000)  # captured right at entry, not after the TP order below
        print(f"      order response: {resp['data']}")
        daily_trade_tracker["count"] += 1

        # Size everything downstream off what actually filled, not what we asked
        # for. Futures MARKET orders can PARTIALLY_EXECUTE with no auto-retry of
        # the remainder — and this strategy specifically targets non-top-100,
        # lower-liquidity coins, so partial fills are a real possibility, not an
        # edge case. Using the requested qty here for the reduce-only TP order
        # (or for P&L bookkeeping) would size it against a position that doesn't
        # actually exist at that size.
        try:
            filled_qty = float(resp["data"].get("exec_quantity", qty))
        except (TypeError, ValueError):
            filled_qty = qty
        if filled_qty <= 0:
            print(f"      {symbol}: order response reports 0 filled quantity, skipping "
                  f"take-profit placement and not tracking a position. Raw: {resp['data']}")
            continue
        if filled_qty != qty:
            print(f"      {symbol}: requested {qty}, filled {filled_qty} "
                  f"(partial fill) — sizing take-profit off the filled amount.")
        # Only deduct the margin actually used (scaled to what filled) from the
        # locally-tracked balance, so this cycle's remaining-balance check
        # reflects the real free margin left, not the fully-requested amount.
        available_balance_usdt -= order_margin_usdt * (filled_qty / qty)
        qty = filled_qty

        entry_msg = (
            f"{'[DRY RUN] ' if DRY_RUN else ''}SHORT {symbol}\n"
            f"Entry: {cand['last_price']} (market)\n"
            f"Qty: {qty}  |  Leverage: {leverage}x"
            f"{f' ({DESIRED_LEVERAGE}x unavailable, capped down)' if leverage < DESIRED_LEVERAGE else ''}"
            f"{f' (symbol minimum forced leverage UP from {DESIRED_LEVERAGE}x)' if leverage > DESIRED_LEVERAGE else ''}\n"
            f"24h: {cand['pct_change_24h']:.2f}%  |  Resistance: ~{resistance:.6g}\n"
            f"No stop-loss set on this position."
        )
        send_telegram_message(entry_msg)

        # Take-profit: target % return on CAPITAL, converted to a price move using
        # THIS trade's actual leverage (which may be below DESIRED_LEVERAGE).
        tp_price_pct = TP_CAPITAL_PCT / leverage
        tp_price = cand["last_price"]
        if TP_CAPITAL_PCT > 0:
            tp_price = round(cand["last_price"] * (1 - tp_price_pct / 100), price_precision)
            tp_resp = place_order(symbol, side="BUY", order_type="LIMIT",
                                   quantity=qty, price=tp_price, reduce_only=True)
            print(f"      take-profit @ {tp_price} "
                  f"({tp_price_pct:.2f}% price move -> {TP_CAPITAL_PCT:.1f}% on capital): {tp_resp['data']}")
            send_telegram_message(
                f"{'[DRY RUN] ' if DRY_RUN else ''}Take-profit set for {symbol} @ {tp_price} "
                f"({tp_price_pct:.2f}% price move -> {TP_CAPITAL_PCT:.1f}% on capital)"
            )

        with state_lock:
            open_shorts[symbol] = {
                "entry_price": cand["last_price"],
                "qty": qty,
                "tp_price": tp_price,
                "opened_at_ms": opened_at_ms,
                "simulated": DRY_RUN,
                "leverage": leverage,                  # needed for the liquidation-distance estimate below
                "liquidation_warning_sent": False,      # tracks whether the 50%-to-liquidation Telegram
                                                         # alert has already fired for this position, so we
                                                         # don't re-send it every single cycle it stays past
                                                         # threshold — see check_liquidation_warnings().
            }
            save_state(open_shorts, daily_trade_tracker)

    # Returned so main()'s loop can carry the (possibly refreshed) market
    # data and refresh-date marker into the next cycle — run_once() itself
    # is stateless between calls otherwise.
    return top_cap_symbols, usdt_inr_rate, last_market_refresh_date, last_status_update_ms


def main():
    print("Fetching top-100 market cap list and USDT/INR rate from CoinGecko...")
    top_cap_symbols = fetch_with_retry(
        get_top_market_cap_symbols, TOP_N_MARKET_CAP_EXCLUDE, description="top-100 market cap list"
    )
    usdt_inr_rate = fetch_with_retry(get_usdt_inr_rate, description="USDT/INR rate")
    # Seeded to today (IST) since we just fetched fresh values above — this
    # stops run_once()'s daily refresh check (bug #6 fix) from immediately
    # re-fetching on its very first cycle.
    last_market_refresh_date = today_ist()
    print(f"USDT/INR ~= {usdt_inr_rate}")

    print("Fetching CoinSwitch futures instrument info...")
    instruments = fetch_with_retry(get_instrument_info, description="CoinSwitch instrument info")

    daily_trade_tracker = {
        "date": today_ist(),
        "count": 0,               # trades opened today
        "trades_closed": 0,
        "wins": 0,
        "losses": 0,
        "realized_pnl_usdt": 0.0,
    }  # resets at midnight IST; a summary is sent to Telegram right before the reset.
       # May be overwritten below by recover_open_positions() if a same-day
       # saved state file exists (restores counters across a restart).

    open_shorts = fetch_with_retry(
        recover_open_positions, instruments, daily_trade_tracker,
        description="recovering open positions from CoinSwitch"
    )  # symbol -> {entry_price, qty, tp_price, opened_at_ms, simulated}; rebuilt from the real
       # account (plus the local state file for bookkeeping the exchange can't provide) on
       # every startup so a restart can't silently forget a still-open position or reset
       # today's trade-count/P&L tracking.

    # Seeded to 0 (not now_ms) so the very first cycle sends an immediate
    # status update if anything got recovered above, instead of waiting a
    # full 15 minutes after every restart before the first snapshot.
    last_status_update_ms = 0

    print(f"DRY_RUN = {DRY_RUN}. Max {MAX_TRADES_PER_DAY} trades/day. "
          f"Starting scan loop every {POLL_INTERVAL_SECONDS}s. Ctrl+C to stop.")
    send_telegram_message(
        f"{'[DRY RUN] ' if DRY_RUN else ''}Bot started. "
        f"Scanning every {POLL_INTERVAL_SECONDS}s, max {MAX_TRADES_PER_DAY} trades/day.\n"
        f"Tap ❌ Close under any position in a status update to close it instantly.\n"
        f"Send /status any time for an on-demand snapshot."
    )

    # Runs the whole time the process is up, independent of the 5-minute scan
    # cycle above — this is what makes a "❌ Close" button tap in Telegram take
    # effect within a second or two instead of waiting for the next scan.
    # Daemon=True so it never blocks process shutdown on its own.
    telegram_thread = threading.Thread(
        target=telegram_polling_loop, args=(open_shorts, daily_trade_tracker), daemon=True
    )
    telegram_thread.start()

    while True:
        try:
            top_cap_symbols, usdt_inr_rate, last_market_refresh_date, last_status_update_ms = run_once(
                instruments, top_cap_symbols, usdt_inr_rate, open_shorts,
                daily_trade_tracker, last_market_refresh_date, last_status_update_ms
            )
        except requests.HTTPError as e:
            print(f"HTTP error this cycle: {e}")
            send_telegram_message(f"⚠️ HTTP error this cycle: {e}")
        except Exception as e:
            print(f"Unexpected error this cycle: {e}")
            send_telegram_message(f"⚠️ Unexpected error this cycle: {e}")
        time.sleep(POLL_INTERVAL_SECONDS)


def run_forever():
    """Outer safety net. main()'s own while-loop already survives per-cycle
    errors (network blips, CoinSwitch 5xxs, etc.) without dying — this exists
    only to catch something escaping that loop entirely (e.g. an error during
    the one-time startup phase that fetch_with_retry doesn't cover, or a bug).
    Missing required env vars (SystemExit from require_env) are a real config
    problem, not a transient failure, so those are allowed to actually exit —
    Railway should surface that as a crashed deployment, not silently loop."""
    while True:
        try:
            main()
        except SystemExit:
            raise
        except Exception as e:
            print(f"[supervisor] main() crashed unexpectedly: {e}. Restarting in 15s...")
            try:
                send_telegram_message(f"⚠️ Bot crashed and is restarting itself: {e}")
            except Exception:
                pass
            time.sleep(15)


if __name__ == "__main__":
    run_forever()
