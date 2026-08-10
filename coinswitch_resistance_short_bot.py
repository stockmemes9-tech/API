"""
CoinSwitch PRO Futures — "Resistance Short" scanner/bot
=========================================================

Strategy (as described by the user):
  Short a coin when ALL of the following are true:
    1. It is NOT in the top-200 cryptos by global market cap.
    2. It is down more than 5% in the last 24 hours.
    3. Its 24h trading volume is between 2 crore (2,00,00,000) and 40 crore (40,00,00,000) INR.
    4. EITHER of the following two independent signals (only one is needed,
       they don't stack):
         4a. On the 15-minute chart, price is currently sitting at a
             resistance level, confirmed by a rejection candle. Declining
             volume into the level is also checked and, when it can be read,
             must actually be declining — but if volume can't be determined
             from the candle data (e.g. the field name doesn't match), this
             check is skipped rather than blocking the trade (see
             is_volume_declining()'s True/False/None logic).
         4b. On the 15-minute chart, RSI(14) is above 77 (see compute_rsi() /
             RSI_OVERBOUGHT_SHORT_THRESHOLD). This path is checked entirely
             on its own — it does NOT require the resistance/rejection-
             candle/declining-volume check in 4a.
    5. The symbol hasn't already been shorted (real or DRY_RUN) in the last hour.

This script:
    - Pulls the global top-200 market-cap list from CoinGecko (free, no key needed)
      and excludes those symbols.
    - Pulls 24h stats for every CoinSwitch futures symbol in one call.
    - Filters by % drop and INR volume (converted from the USDT volume CoinSwitch
      reports, using a live USDT/INR rate from CoinGecko).
    - For each surviving symbol, pulls 15m candles and looks for swing-high
      ("pivot high") resistance levels, then checks whether the current price
      is sitting just under one of them, requiring a rejection wick AND
      declining volume into the level (see is_volume_declining()) before
      treating it as confirmed.
    - Skips any symbol that was already entered within the last hour,
      even if it closed and re-qualifies again in the meantime (see
      ENTRY_COOLDOWN_HOURS / daily_trade_tracker["recent_entries"]).
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
    - "Not in top 200 by market cap" is matched by ticker symbol against
      CoinGecko's top 200. Ticker symbols can collide across unrelated coins
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
import csv
import sys
import signal
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

# Set by the /pause command, cleared by /resume (see telegram_polling_loop()).
# Gates ONLY the opening of new trades in run_once() — reconciliation,
# liquidation warnings, status updates, and manual closes all keep working
# normally while paused, so you can still monitor and exit existing
# positions, you just won't get new entries.
bot_paused = threading.Event()

# Tracks consecutive price-fetch failures across BOTH the 5-minute scan loop
# (run_once) and the fast price_monitor_loop thread, since they hit the same
# underlying CoinSwitch endpoint and a real outage will show up in both. Also
# doubles as the "last known good" timestamp the heartbeat reports on. See
# record_fetch_success()/record_fetch_failure().
connectivity_lock = threading.Lock()
connectivity_state = {
    "consecutive_failures": 0,
    "alert_sent": False,
    "last_success_ms": None,
    "first_failure_ms": None,
    "last_error": None,
}

# Populated by main() once open_shorts/daily_trade_tracker exist, so the
# signal handler below (which fires on the main thread, asynchronously,
# whenever the process gets SIGTERM/SIGINT) has something to save. See
# _handle_shutdown_signal().
_shutdown_context = {"open_shorts": None, "daily_trade_tracker": None}


def _handle_shutdown_signal(signum, frame):
    """Registered for SIGTERM (what Railway sends on redeploy/restart/stop)
    and SIGINT (Ctrl+C locally). Without this, a mid-cycle kill can lose any
    state changes since the last save_state() call — e.g. a position closed
    seconds ago, or today's running P&L — since those live only in memory
    between saves. This makes shutdown itself a save point instead of a gap.

    Deliberately best-effort and fast: grabs state_lock, writes the state
    file, fires one Telegram notice, and exits. Doesn't try to close open
    positions or do anything else that could take a while or fail loudly —
    the point is to preserve bookkeeping, not to trade on the way out."""
    sig_name = signal.Signals(signum).name
    print(f"\n[shutdown] received {sig_name}, saving state before exiting...")

    open_shorts = _shutdown_context.get("open_shorts")
    daily_trade_tracker = _shutdown_context.get("daily_trade_tracker")
    saved_ok = False
    if open_shorts is not None and daily_trade_tracker is not None:
        try:
            with state_lock:
                save_state(open_shorts, daily_trade_tracker)
            saved_ok = True
            print("[shutdown] state saved.")
        except Exception as e:
            print(f"[shutdown] failed to save state: {e}")

    try:
        send_telegram_message(
            f"🛑 Bot received {sig_name} and is shutting down.\n"
            + (f"State saved ({len(open_shorts)} open position(s) preserved)."
               if saved_ok else "⚠️ State save failed or hadn't started yet — check logs.")
        )
    except Exception as e:
        print(f"[shutdown] failed to send Telegram notice: {e}")

    try:
        backup_trade_history()
    except Exception as e:
        print(f"[shutdown] failed to back up trade history: {e}")

    sys.exit(0)


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
TOP_N_MARKET_CAP_EXCLUDE = 200
MIN_24H_DROP_PCT = 5.0                # price must be down at least this much
MIN_24H_VOLUME_INR = 2_00_00_000      # 2 crore INR
MAX_24H_VOLUME_INR = 40_00_00_000     # 40 crore INR — coins above this are excluded too (the idea being
                                       # very high-volume/liquid coins on this non-top-200 screen behave
                                       # differently from the thinner, choppier names this bot is meant
                                       # to trade — see screen_candidates()).

# --- Resistance detection (15m chart) ---
KLINE_INTERVAL = "15"                 # minutes; "15" = 15m candles
RESISTANCE_LOOKBACK_CANDLES = 150     # ~37.5 hours of 15m candles
PIVOT_WING = 3                        # candles on each side to confirm a swing high
RESISTANCE_TOLERANCE_PCT = 0.4        # "at resistance" = within this % of a swing-high cluster
REQUIRE_REJECTION_CANDLE = True       # Strategy 1 only — also require the latest candle to show a
                                       # rejection wick before shorting. Strategy 3 (below) skips this
                                       # requirement and shorts as soon as price touches the level.

# Rule: only short if volume is FADING as price pushes up into the resistance
# level, not rising into it. Rising volume into a level more often precedes a
# breakout through it; declining volume suggests buyers are running out of
# steam right at the level, which is the setup this bot is meant to catch.
# See is_volume_declining() for exactly how "declining" is measured.
REQUIRE_DECLINING_VOLUME = True
VOLUME_DECLINE_LOOKBACK_CANDLES = 6   # ~90 minutes of 15m candles looked at for the volume trend
VOLUME_DECLINE_MIN_PCT = 15.0         # 2nd half of that window must average at least this much
                                       # lower volume than the 1st half to count as "declining"
                                       # (a tiny/noisy dip shouldn't be enough to pass the filter)

# --- RSI overbought short trigger (15m chart) ---
# A SECOND, independent entry path alongside the resistance setup above. A
# candidate that already passed rule 1-3 screening (not top-200, down >5% in
# 24h, 2cr-40cr INR volume) is also shorted if its 15m-candle RSI is above the
# threshold below — even if it never shows a confirmed resistance rejection.
# Per your instruction: base screening still applies, but the
# resistance/rejection-candle/declining-volume check is skipped entirely for
# this path (see run_once(), where resistance and RSI are checked as two
# separate ways to arrive at the same SHORT signal, not stacked together).
RSI_SHORT_ENABLED = True
RSI_PERIOD = 14                       # standard Wilder RSI lookback, in 15m candles
RSI_OVERBOUGHT_SHORT_THRESHOLD = 77.0 # RSI strictly above this on the latest closed 15m candle triggers a short

# ============================ STRATEGY 2 (RSI 80/20) ========================
# A second, completely separate strategy. Only one strategy is ever ACTIVE
# for opening NEW trades at a time (switchable live via the /strategy1 and
# /strategy2 Telegram commands, see strategy_state below) — but positions
# already open from either strategy keep being reconciled, monitored, and
# can still be closed regardless of which strategy is currently active.
# Strategy 1's own rules/thresholds above are completely untouched by any
# of this.
#
# Strategy 2 rules:
#   - Screening: ONLY "not in the top-200 by global market cap" (same list
#     strategy 1 uses). No 24h-drop-%, and deliberately NO 24h-volume check
#     (per instruction) — every non-top-200 CoinSwitch futures symbol is a
#     candidate.
#   - Entry, on the 1-hour chart:
#       RSI(14) above STRATEGY2_RSI_OVERBOUGHT (80) -> SHORT
#       RSI(14) below STRATEGY2_RSI_OVERSOLD (20)    -> LONG
#   - Leverage: same DESIRED_LEVERAGE (5x) target as strategy 1, falling
#     back to the highest leverage the symbol actually allows if 5x isn't
#     available (reuses resolve_leverage(), unchanged).
#   - Take-profit: STRATEGY2_TP_CAPITAL_PCT (1%) of the CAPITAL employed on
#     that trade — i.e. 1% of the margin (CAPITAL_INR), NOT 1% of the
#     leveraged notional value. Same "% on capital -> % price move" style of
#     conversion as strategy 1's TP_CAPITAL_PCT, just a different number and
#     usable in either direction (short: price down; long: price up).
STRATEGY2_ENABLED = True
STRATEGY2_RSI_OVERBOUGHT = 80.0       # RSI strictly above this on the 1h chart -> SHORT
STRATEGY2_RSI_OVERSOLD = 20.0         # RSI strictly below this on the 1h chart -> LONG
STRATEGY2_TP_CAPITAL_PCT = 1.0        # target: 1% profit on capital employed (not on leveraged notional)
STRATEGY2_KLINE_INTERVAL = "60"       # minutes; "60" = 1h candles (strategy 1 stays on 15m, unchanged)
STRATEGY2_LOOKBACK_CANDLES = 100      # ~4 days of 1h candles — plenty for a 14-period RSI

# ============================ STRATEGY 3 (resistance, no confirmation) ======
# A third, separate strategy. Same base screening (rules 1-3, identical to
# strategy 1: not top-200, down >5% in 24h, 2cr-40cr INR volume) and the same
# resistance-level detection (find_resistance_levels() / is_at_resistance()),
# but two differences from strategy 1's path 4a:
#   - Does NOT require a confirmed rejection candle (has_rejection_candle())
#     before entering — shorts as soon as price is within
#     RESISTANCE_TOLERANCE_PCT of a detected level, one candle earlier than
#     strategy 1 would.
#   - Actually places a SHORT (SELL) on trigger, unlike strategy 1's
#     enter_trades_strategy1(), which places a LONG (BUY) despite the
#     resistance/RSI-overbought signal being a short setup.
# Strategy 3 does NOT use the RSI-overbought path (4b) — resistance-only.
# Volume-decline filtering is still applied (STRATEGY3_REQUIRE_DECLINING_VOLUME),
# using the same REQUIRE_DECLINING_VOLUME thresholds/lookback as strategy 1.
# Take-profit and leverage/sizing rules are identical in shape to strategy 1's
# (same DESIRED_LEVERAGE, same CAPITAL_INR per trade), just its own % target.
STRATEGY3_ENABLED = True
STRATEGY3_REQUIRE_DECLINING_VOLUME = True   # set False to also skip the volume-decline filter
STRATEGY3_TP_CAPITAL_PCT = 5.0        # target: 5% profit on capital employed, same style as TP_CAPITAL_PCT

# Break-and-reject confirmation (replaces the old bare-touch entry):
#   Candle N:   CLOSES ABOVE a resistance level -> arms a pending
#               confirmation for that symbol/level, does NOT enter yet.
#   Candle N+1: the very next closed 15m candle for that symbol CLOSES
#               BELOW the SAME level -> confirmed, enter SHORT this cycle.
# If candle N+1 does not close back below the level (or a gap in candle
# history means it isn't really the immediate next candle), the pending
# confirmation is discarded rather than entering. See
# get_strategy3_confirmed_resistance() and strategy3_pending_confirmation
# below.
STRATEGY3_REQUIRE_TWO_CANDLE_CONFIRMATION = True

# ============================ STRATEGY 4 (BTC 15m EMA9 flip) ================
# A fourth, completely separate strategy. Unlike strategies 1-3, this does
# NOT run any market-wide screening at all (no top-200 exclusion, no 24h
# drop/volume filter) — it always trades exactly one fixed symbol
# (STRATEGY4_SYMBOL, default BTCUSDT) and is meant to flip position as often
# as its signal changes, taking as many trades as the 15m chart gives it in
# a day.
#
# Rule, both directions off the SAME 15m EMA9 line, always on the LATEST
# CLOSED 15m candle (never the still-forming live candle):
#   - flat + latest closed candle CLOSES ABOVE EMA9 -> go LONG
#   - flat + latest closed candle CLOSES BELOW EMA9 -> go SHORT
# Each position closes on WHICHEVER of these happens first:
#   (a) its take-profit: STRATEGY4_TP_PRICE_MOVE_PCT (0.3%) price move in
#       its favor (a flat price-move %, NOT a %-on-capital figure like
#       strategies 1-3 use) — a resting reduce-only limit order, same as
#       the other strategies place.
#   (b) a later closed 15m candle closes back across EMA9 AGAINST the open
#       position (LONG open + close < EMA9, or SHORT open + close > EMA9).
#       This is checked every cycle by check_strategy4_signal_exits(),
#       independent of whichever strategy is currently ACTIVE for new
#       entries, same as reconcile_open_shorts()/check_liquidation_warnings()
#       above — an open Strategy 4 position keeps getting this check even if
#       you switch to Strategy 1/2/3 in the meantime.
# Leverage: fixed at STRATEGY4_LEVERAGE (7x), falling back to the highest
# leverage BTCUSDT actually allows if 7x isn't available (reuses
# resolve_leverage(), unchanged, just with a different `desired`).
# Capital: STRATEGY4_CAPITAL_INR, floored at 10,000 INR per your instruction
# ("minimum 10000 rupees as capital") even if someone sets the env var lower.
#
# Deliberately exempt from the shared ENTRY_COOLDOWN_HOURS / LOSS_COOLDOWN_HOURS
# re-entry cooldowns and the shared MAX_TRADES_PER_DAY cap that strategies 1-3
# use (see enter_trades_strategy4()) — the only thing stopping a new entry
# here is already having an open position on STRATEGY4_SYMBOL, or (in live
# trading) not having enough free wallet balance for the next trade's margin.
STRATEGY4_ENABLED = True
STRATEGY4_SYMBOL = os.environ.get("STRATEGY4_SYMBOL", "BTCUSDT").strip().upper()
STRATEGY4_EMA_PERIOD = 9
STRATEGY4_KLINE_INTERVAL = "15"        # minutes; 15m chart, same granularity as strategy 1/3
STRATEGY4_LOOKBACK_CANDLES = 150       # plenty of candles for a stable EMA9 seed
STRATEGY4_LEVERAGE = 7
STRATEGY4_CAPITAL_INR = max(10_000, int(os.environ.get("STRATEGY4_CAPITAL_INR", "10000")))
STRATEGY4_TP_PRICE_MOVE_PCT = 0.3      # flat price-move %, not a %-on-capital figure

# ============================ STRATEGY 5 ("RE Strategy", EMA9/21 cross) =====
# A fifth, completely separate strategy — ported live from the standalone
# backtest_strategy_ema9_ema21_cross.py script. Like strategy 4, this does
# NOT run any market-wide screening — it only ever trades the fixed list of
# symbols in STRATEGY5_SYMBOLS (default REUSDT,CCUSDT,DEEPUSDT,CRVUSDT,
# ARBUSDT,TREEUSDT,PLUMEUSDT,AEROUSDT,ARXUSDT,EIGENUSDT —
# REUSDT matches the backtest script's own default; CCUSDT/
# DEEPUSDT/CRVUSDT/ARBUSDT/TREEUSDT/PLUMEUSDT/AEROUSDT/ARXUSDT/EIGENUSDT were
# added on top of it (SAHARAUSDT was removed after repeated exchange-side
# "Insufficient balance" rejections that didn't match its actual free
# wallet balance — see notes near STRATEGY5_FEE_BUFFER_USDT; RIFUSDT was
# removed after it returned no candles on every single cycle). Each symbol
# in the list is evaluated
# independently every cycle and can have its own open position at the same
# time.
#
# Rule, evaluated on the latest CLOSED candle only (see
# compute_ema_cross_signal()):
#   - flat + EMA9 crosses ABOVE EMA21 on that candle, AND that same candle's
#     CLOSE is above BOTH EMA9 and EMA21 (closes on the bullish side of the
#     crossover, not back through it) -> go LONG
#   - flat + EMA9 crosses BELOW EMA21 on that candle, AND that same candle's
#     CLOSE is below BOTH EMA9 and EMA21 -> go SHORT
#   - A crossover event fires exactly once, on the bar it happens on — not
#     every bar afterward. If the crossover candle closes back on the
#     "wrong" side (a whipsaw/doji-type bar), the signal is dropped for that
#     bar, not deferred to a later one — a fresh crossover is required to
#     try again.
#
# UNLIKE strategy 4, there is no signal-reversal exit here. A position closes
# ONLY on whichever of its take-profit or stop-loss resting orders fills
# first (STRATEGY5_TP_PCT / STRATEGY5_SL_PCT, both flat price-move %s off
# entry — same "TP or SL, whichever hits first, no opposite-signal exit"
# rule as the backtest). Both orders are placed at entry time (see
# enter_trades_strategy5()); reconcile_open_shorts() already knows how to
# check both a tp_price and sl_price for DRY_RUN positions, and generically
# detects a real position's closure regardless of which order filled it.
#
# Leverage: fixed at STRATEGY5_LEVERAGE, falling back to the highest leverage
# the symbol actually allows if that's not available (resolve_leverage(),
# same as strategies 2/4). Capital: sized per-trade in the
# STRATEGY5_MIN_TRADE_USDT-STRATEGY5_MAX_TRADE_USDT USDT range (default
# 70-76 USDT) rather than an INR figure converted at the live rate — set
# directly in USDT since this strategy's wallet/trade-size requirement was
# specified in USDT, not INR. Entries are placed as MARKET orders, filling
# close to the latest closed candle's close (i.e. the current price).
#
# Deliberately exempt from the shared ENTRY_COOLDOWN_HOURS/LOSS_COOLDOWN_HOURS
# re-entry cooldowns and MAX_TRADES_PER_DAY cap, same reasoning as strategy 4
# — the only real gate is already having an open position on this symbol, or
# (in live trading) not having enough free wallet balance for the margin.
STRATEGY5_ENABLED = True
# Comma-separated list of symbols this strategy trades, e.g.
# "REUSDT,CCUSDT,DEEPUSDT,CRVUSDT,ARBUSDT,TREEUSDT,PLUMEUSDT,AEROUSDT,
# ARXUSDT,EIGENUSDT". Override via the STRATEGY5_SYMBOLS env var (still
# comma-separated). STRATEGY5_SYMBOL (singular) is kept as a
# backward-compatible override for a single-symbol deploy — if it's set and
# STRATEGY5_SYMBOLS isn't, it's used instead of the default list.
_STRATEGY5_SYMBOLS_DEFAULT = "CCUSDT,DEEPUSDT,CRVUSDT,ARBUSDT,PLUMEUSDT,AEROUSDT,ARXUSDT,EIGENUSDT,REZUSDT,ADXUSDT,DGBUSDT,OPNUSDT"
STRATEGY5_SYMBOLS = [
    s.strip().upper()
    for s in os.environ.get(
        "STRATEGY5_SYMBOLS",
        os.environ.get("STRATEGY5_SYMBOL", _STRATEGY5_SYMBOLS_DEFAULT),
    ).split(",")
    if s.strip()
]
STRATEGY5_EMA_FAST = 9
STRATEGY5_EMA_SLOW = 21
# Minutes per candle. NOTE: the backtest script's docstring describes a DAILY
# (1440) default, but its actual --interval argparse default is "60" (1h) —
# this mirrors that real default, not the docstring, since "60" is almost
# certainly what was actually backtested. Override via STRATEGY5_KLINE_INTERVAL
# env var if you want to run this on a different timeframe (e.g. "1440" for
# daily — unverified against CoinSwitch's current API, see the backtest
# script's caveats).
STRATEGY5_KLINE_INTERVAL = os.environ.get("STRATEGY5_KLINE_INTERVAL", "60").strip()
STRATEGY5_LOOKBACK_CANDLES = 150       # plenty of candles for a stable EMA21 seed
STRATEGY5_LEVERAGE = 10                # matches the backtest's --leverage default
# Per-trade margin range, in flat USDT (not INR-converted). The minimum
# wallet balance required before even attempting a trade is
# STRATEGY5_MIN_TRADE_USDT. The actual trade size is whatever's free in the
# wallet, capped at STRATEGY5_MAX_TRADE_USDT, so it naturally lands in the
# 70-76 USDT range.
STRATEGY5_MIN_TRADE_USDT = float(os.environ.get("STRATEGY5_MIN_TRADE_USDT", "70"))
STRATEGY5_MAX_TRADE_USDT = float(os.environ.get("STRATEGY5_MAX_TRADE_USDT", "70"))
# Reserved headroom, subtracted from the live balance BEFORE capping at
# STRATEGY5_MAX_TRADE_USDT, so a trade sized off the full free balance
# doesn't leave zero room for CoinSwitch's taker fee on entry (observed
# live: a balance only just above the margin figure still got rejected as
# "Insufficient balance" because the fee had nowhere left to come from).
STRATEGY5_FEE_BUFFER_USDT = float(os.environ.get("STRATEGY5_FEE_BUFFER_USDT", "1.5"))
STRATEGY5_TP_PCT = 5.0                 # flat price-move %, matches backtest --tp-pct default
STRATEGY5_SL_PCT = 5.0                 # flat price-move %, matches backtest --sl-pct default (0 disables)
# Re-entry cooldown, specific to strategy 5: after a symbol's position closes
# (TP, SL, or manual — any reason), that symbol is skipped for this many
# hours from the close time, even if a fresh EMA9/EMA21 crossover fires on
# it in the meantime. Unlike ENTRY_COOLDOWN_HOURS/LOSS_COOLDOWN_HOURS (which
# strategy 5 is otherwise exempt from — see enter_trades_strategy5()'s
# docstring), this one is strategy-5-only and applies regardless of whether
# the trade closed as a win or a loss. Keyed off
# daily_trade_tracker["recent_closes"] (symbol -> closed_at_ms), stamped by
# record_recent_close() at every close path.
STRATEGY5_REENTRY_COOLDOWN_HOURS = float(os.environ.get("STRATEGY5_REENTRY_COOLDOWN_HOURS", "1"))
STRATEGY5_REENTRY_COOLDOWN_MS = int(STRATEGY5_REENTRY_COOLDOWN_HOURS * 60 * 60 * 1000)

# Quick-tap TP/SL percentages shown as inline buttons under each open
# position in /status and the periodic status update (see
# send_position_status_update()). These are flat price-move percentages —
# for a USDT-margined linear perp, price-move % IS notional-value % (PnL =
# price_move_pct * notional, independent of leverage), so "5%" here means a
# take-profit/stop-loss 5% away from entry in price terms, same convention
# STRATEGY5_TP_PCT/STRATEGY5_SL_PCT already use. Tapping one replaces any
# existing TP/SL for that position via set_take_profit_manual()/
# set_stop_loss_manual() — same code path as typing /tp or /sl manually.
QUICK_TPSL_PCTS = [2, 5, 10]

# symbol -> {"level": float, "candle_ts": <last confirmed candle's start_time>}
# Only ever touched by the main scan-loop thread (enter_trades_strategy3()),
# not by telegram_polling_loop, so unlike open_shorts/daily_trade_tracker it
# doesn't need state_lock. Persisted in the state file (see save_state/
# load_state) purely so a restart between candle N and candle N+1 doesn't
# silently drop an in-progress confirmation.
strategy3_pending_confirmation = {}

# Which strategy is currently allowed to open NEW trades: "1", "2", or "3".
# Read from env as the default on a fresh deploy, then overridable live via
# /strategy1 /strategy2 /strategy3 in Telegram and persisted across restarts
# in the state file (see save_state/load_state and strategy_state below) —
# the env var only matters until the first Telegram switch or a saved state
# file is found.
ACTIVE_STRATEGY_DEFAULT = os.environ.get("ACTIVE_STRATEGY", "1").strip()
if ACTIVE_STRATEGY_DEFAULT not in ("1", "2", "3", "4", "5"):
    ACTIVE_STRATEGY_DEFAULT = "1"

# Mutable "which strategy is active" holder, guarded by state_lock same as
# open_shorts/daily_trade_tracker (telegram_polling_loop's /strategy1,
# /strategy2, /strategy3 handlers mutate this from a different thread than
# the scan loop reads it from).
strategy_state = {"active": ACTIVE_STRATEGY_DEFAULT}

STRATEGY_NAMES = {
    "1": "resistance/RSI(77) LONG-only",
    "2": "RSI(14) 80/20 on 1h SHORT+LONG",
    "3": "resistance, close-above-then-close-below confirmation, SHORT-only",
    "4": f"{STRATEGY4_SYMBOL} 15m EMA9 flip, LONG+SHORT, {STRATEGY4_LEVERAGE}x leverage",
    "5": f"RE Strategy — {', '.join(STRATEGY5_SYMBOLS)} {STRATEGY5_KLINE_INTERVAL}m EMA9/EMA21 cross, "
         f"LONG+SHORT, {STRATEGY5_LEVERAGE}x leverage, {STRATEGY5_TP_PCT:g}% TP / {STRATEGY5_SL_PCT:g}% SL",
}

# =============================================================================

# --- Order sizing / risk ---
CAPITAL_INR = 10_000                  # fixed margin per trade, in INR (converted to USDT at runtime)
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

MAX_TRADES_PER_DAY = 25               # hard cap on new entries per calendar day (resets at midnight IST)
                                       # No cap on concurrent open positions — the bot will keep as many
                                       # open at once as the daily trade count and available wallet
                                       # balance allow.

# Rule: never open a NEW short on a symbol that was already entered (real or
# DRY_RUN) within the last this-many hours — even if it closed in the
# meantime and re-qualifies later the same cycle or a later one. This is a
# rolling window per symbol, NOT tied to the IST calendar day the way
# MAX_TRADES_PER_DAY is, so it survives a midnight rollover correctly (e.g.
# a short opened at 23:50 IST still blocks re-entry for 1h into the next
# morning).
# See daily_trade_tracker["recent_entries"] (symbol -> opened_at_ms) below.
ENTRY_COOLDOWN_HOURS = 1
ENTRY_COOLDOWN_MS = ENTRY_COOLDOWN_HOURS * 60 * 60 * 1000

# Separate, longer cooldown that only kicks in when the symbol's most recent
# closed trade (any strategy, any close reason — stop-loss, take-profit,
# manual Telegram close, or exchange-detected close) was a LOSS. A winning
# close still only blocks re-entry for ENTRY_COOLDOWN_HOURS above; a losing
# close blocks re-entry for this much longer, on the theory that a setup
# that just failed on this symbol is more likely to fail again soon than to
# suddenly start working. Rolling window per symbol, same style as
# ENTRY_COOLDOWN_MS/recent_entries — survives the midnight-IST rollover.
# See daily_trade_tracker["recent_losses"] (symbol -> closed_at_ms) below.
LOSS_COOLDOWN_HOURS = 48
LOSS_COOLDOWN_MS = LOSS_COOLDOWN_HOURS * 60 * 60 * 1000

POLL_INTERVAL_SECONDS = 300           # rescan cadence (independent of the 15m candle interval)

# --- Position monitoring ---
STATUS_UPDATE_INTERVAL_SECONDS = 15 * 60   # send an open-positions P&L snapshot to Telegram this often
LIQUIDATION_WARNING_PCT = 50.0             # alert once a position's adverse move has covered this
                                            # % of the distance from entry to its estimated liquidation
                                            # price (see estimate_liquidation_price() for the caveats
                                            # on how that estimate is derived).
LOSS_ALERT_PCT = 30.0                      # alert once a position's unrealized loss (as a % of the
                                            # margin actually put up for that trade, i.e. price move %
                                            # times leverage) reaches this. Independent of, and usually
                                            # fires well before, LIQUIDATION_WARNING_PCT.
PRICE_MONITOR_INTERVAL_SECONDS = 30        # how often the dedicated fast-monitor thread (separate
                                            # from the 5-minute scan cycle) re-checks open positions'
                                            # live prices for the loss/liquidation alerts above — see
                                            # price_monitor_loop().
CONNECTIVITY_ALERT_THRESHOLD = 3           # consecutive price-fetch failures (across the scan loop
                                            # AND the fast monitor combined) before sending a "we're
                                            # flying blind" Telegram alert — see record_fetch_failure().
HEARTBEAT_INTERVAL_SECONDS = 60 * 60       # how often heartbeat_loop() sends a "still alive" ping,
                                            # independent of the daily summary and any other alert.

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

# Every trade that closes (auto TP, exchange-detected close, or manual /
# Telegram-button close) gets one row appended here — see record_trade_close()
# and the /history command in telegram_polling_loop(). Same ephemeral-on-Railway
# caveat as STATE_FILE_PATH applies unless a volume is mounted.
TRADE_HISTORY_FILE_PATH = os.environ.get("TRADE_HISTORY_FILE_PATH", "trade_history.csv")

# =============================================================================


# ------------------------- CoinSwitch auth helper ---------------------------
# From CoinSwitch's official Reference Client docs.
def sign_request(method, path, params=None):
    method = method.upper()
    encoded_path = path
    if params:
        sep = "&" if "?" in path else "?"
        encoded_path = path + sep + urllib.parse.urlencode(params)
    # CORE BUG FIX: this used to unquote_plus() the encoded path and then
    # return THAT (decoded) path as the actual request URL too — which
    # silently undid urlencode()'s escaping before the request ever hit the
    # network. Harmless for params with no reserved characters (symbols,
    # timestamps, etc — the vast majority of calls), but for any param
    # value containing '&', '=', '#', '%', or a space, it corrupted the
    # query string. Concretely: get_realized_pnl()'s "type": "P&L" param
    # encodes correctly to "type=P%26L", then got unquote_plus()'d back to
    # the literal "type=P&L" — which the server parses as type=P + a bare
    # "L" token, not "type=P&L", producing a 400 Bad Request on every
    # single call. That silent failure was masking the bot's own trading
    # fees: get_realized_pnl() always fell back to a price-only estimate
    # (which explicitly excludes fees — see the "(fees excluded)" log
    # line), so every P&L shown in Telegram/status was missing entry/exit
    # fees, making the wallet balance drift lower than reported gains would
    # suggest.
    #
    # decoded_path is still computed and still used for the SIGNATURE
    # message below (unchanged behavior — every currently-working endpoint
    # keeps signing over the exact same decoded string it always has,
    # since none of their param values contain reserved characters, decoded
    # and encoded forms are identical for them). Only the path actually
    # sent to the network changes, from the corrupted decoded_path to the
    # correct encoded_path.
    decoded_path = urllib.parse.unquote_plus(encoded_path)

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
    return headers, encoded_path


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
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            # requests doesn't raise on a non-2xx response unless you call
            # raise_for_status() — without this check, a Telegram-side
            # rejection (rate limit, bad chat id, message too long, etc.)
            # was silently swallowed: no exception, no log, no alert ever
            # delivered, even though the caller thought this "succeeded".
            print(f"  [telegram] send failed: HTTP {r.status_code}, body: {r.text[:500]}")
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


def send_telegram_document(file_path, caption=""):
    """Sends a file (used for attaching the full trade-history CSV to a
    /history reply) to the configured chat. Best-effort, same as
    send_telegram_message — a failed attachment never crashes the caller."""
    if not ENABLE_TELEGRAM_NOTIFICATIONS or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
        with open(file_path, "rb") as f:
            requests.post(
                url,
                data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption},
                files={"document": (os.path.basename(file_path), f)},
                timeout=30,
            )
    except Exception as e:
        print(f"  [telegram] failed to send document: {e}")


# ------------------------------ Connectivity health ---------------------------

def format_duration(seconds):
    """Human-readable duration for heartbeat/connectivity messages, e.g. '2h 14m' or '43s'."""
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {seconds}s"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes}m"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h"


def record_fetch_success():
    """Call after any successful get_all_tickers() call. Resets the failure
    streak and, if an outage alert had been sent, sends a matching recovery
    alert so you know the moment it clears rather than having to infer it
    from alerts simply stopping."""
    with connectivity_lock:
        now_ms = int(time.time() * 1000)
        connectivity_state["last_success_ms"] = now_ms
        if connectivity_state["alert_sent"]:
            down_seconds = (now_ms - connectivity_state["first_failure_ms"]) / 1000
            failures = connectivity_state["consecutive_failures"]
            send_telegram_message(
                f"🟢 API connectivity restored — price fetches succeeding again "
                f"after {failures} consecutive failures over ~{format_duration(down_seconds)}."
            )
        connectivity_state["consecutive_failures"] = 0
        connectivity_state["alert_sent"] = False
        connectivity_state["first_failure_ms"] = None
        connectivity_state["last_error"] = None


def record_fetch_failure(source, error):
    """Call after any failed get_all_tickers() call, from either the scan
    loop or the fast price monitor. Once CONNECTIVITY_ALERT_THRESHOLD
    consecutive failures pile up (across both callers combined — they hit
    the same endpoint) sends a single alert, not one per failure, so a rough
    patch of network flakiness doesn't spam the chat."""
    with connectivity_lock:
        connectivity_state["consecutive_failures"] += 1
        connectivity_state["last_error"] = str(error)
        if connectivity_state["first_failure_ms"] is None:
            connectivity_state["first_failure_ms"] = int(time.time() * 1000)
        if (connectivity_state["consecutive_failures"] >= CONNECTIVITY_ALERT_THRESHOLD
                and not connectivity_state["alert_sent"]):
            send_telegram_message(
                f"🔴 API connectivity issue: {connectivity_state['consecutive_failures']} "
                f"consecutive price-fetch failures (most recently from {source}).\n"
                f"Last error: {connectivity_state['last_error']}\n"
                f"Loss/liquidation alerts and new entries may be delayed or skipped until this clears."
            )
            connectivity_state["alert_sent"] = True


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

def get_all_tickers(max_retries=2, retry_delay_seconds=2.0):
    """GET is idempotent, so unlike place_order() below it's safe to retry
    this on ANY transient failure — rate limiting (429) or a dropped
    connection/timeout — not just 429. A retry can never double-submit
    anything here."""
    headers, path = sign_request(
        "GET", "/trade/api/v2/futures/all-pairs/ticker", {"exchange": EXCHANGE}
    )
    for attempt in range(max_retries + 1):
        try:
            r = requests.get(BASE_URL + path, headers=headers, timeout=15)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < max_retries:
                wait = retry_delay_seconds * (2 ** attempt)
                print(f"  [tickers] network error ({e}), retrying in {wait:.1f}s...")
                time.sleep(wait)
                continue
            raise
        if r.status_code == 429 and attempt < max_retries:
            wait = retry_delay_seconds * (2 ** attempt)
            print(f"  [tickers] rate-limited (429), retrying in {wait:.1f}s...")
            time.sleep(wait)
            continue
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
        positions = body["data"]
        # Defensive filter: confirmed live that CoinSwitch's ?symbol= filter
        # can't be trusted — it was seen returning the account's ONE real
        # open position (e.g. DEEPUSDT) for every symbol queried, regardless
        # of the symbol param sent. Without this check, recover_open_positions()
        # blindly took positions[0] as if it belonged to the queried symbol,
        # fabricating a "position" on 9 different symbols all sharing the
        # same entry price/qty as the one real position — untrackable and
        # uncloseable since there was never a real position there to close.
        # Only trust an entry whose OWN symbol field actually matches what
        # was requested; entries missing a symbol field are kept as-is since
        # there's nothing to cross-check them against.
        filtered = []
        mismatched = []
        for p in positions:
            p_symbol = p.get("symbol") or p.get("instrument") or p.get("instrument_name")
            if p_symbol is None or str(p_symbol).upper() == symbol.upper():
                filtered.append(p)
            else:
                mismatched.append(p_symbol)
        if mismatched:
            print(f"  [positions] {symbol}: API returned position(s) for a DIFFERENT symbol "
                  f"({', '.join(str(m) for m in mismatched)}) — discarding them instead of "
                  f"mistracking them as {symbol}'s position. This means CoinSwitch's symbol "
                  f"filter isn't reliable; every get_positions() caller is now protected, but "
                  f"consider filing this with CoinSwitch.")
        return filtered


def confirm_fill_via_positions(symbol, max_attempts=4, delay_seconds=2.0):
    """CORE FIX for the "0 filled quantity" bug: place_order()'s own response
    for a MARKET order can report status='RAISED' / exec_quantity='0' even
    though the order goes on to fill on the exchange a moment later — the
    response is captured before CoinSwitch's matching engine has actually
    processed it, not after. Treating that snapshot as final was the root
    cause of a live TREEUSDT entry (2026-08-10) that filled and was visible
    in the CoinSwitch app, while the bot concluded it hadn't filled, never
    tracked it, and never placed a TP/SL — leaving a real, unprotected
    position invisible to /status and everything else.

    This polls the authoritative source of truth (GET /futures/positions,
    via get_positions()) a few times with a short delay, instead of trusting
    the immediate order-response snapshot. If a real position for `symbol`
    shows up with nonzero size, the order DID fill — return that quantity.
    If nothing shows up after all attempts, it genuinely didn't fill.

    Returns the filled quantity (float, always positive) if a position is
    found, else None. Never raises: a get_positions() error on one attempt
    just costs that attempt, it doesn't abort the whole poll."""
    for attempt in range(max_attempts):
        time.sleep(delay_seconds)
        try:
            positions = get_positions(symbol)
        except Exception as e:
            print(f"      [fill-check] {symbol}: get_positions() failed on attempt "
                  f"{attempt + 1}/{max_attempts} ({e}), retrying...")
            continue
        if positions:
            p = positions[0]
            qty = None
            # Same field-name fallback order as recover_open_positions(),
            # since this is the same /futures/positions schema.
            for key in ("position_size", "quantity", "size", "position_amount", "qty"):
                try:
                    qty = abs(float(p[key]))
                    break
                except (KeyError, ValueError, TypeError):
                    continue
            if qty:
                print(f"      [fill-check] {symbol}: position found on attempt "
                      f"{attempt + 1}/{max_attempts} with size {qty} — the order DID fill, "
                      f"despite the immediate order response reporting 0 filled quantity.")
                return qty
    print(f"      [fill-check] {symbol}: no open position found after {max_attempts} attempts "
          f"({max_attempts * delay_seconds:.0f}s total) — order genuinely did not fill.")
    return None


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


def get_account_transactions(from_time_ms=None, to_time_ms=None, limit=100):
    """Fetches every balance-affecting transaction on the futures account
    (across all symbols) — trading fees (COMMISSION), funding payments
    (FUNDING_FEE), realized P&L (P&L), liquidation fees (LIQUIDATION_FEE),
    and margin top-ups (ADD_MARGIN) — per CoinSwitch's Get Transactions
    endpoint. No 'type' filter is passed, so all of the above come back
    together in one call; summarize_fees_and_pnl() below buckets them.

    limit defaults to 100 rather than a large round number — CoinSwitch
    doesn't publish a max for this param, and a too-high value was
    observed to make the whole request come back 400 Bad Request.

    from_time_ms/to_time_ms omitted entirely means "no bound on that side" —
    used for the /fees command's all-time totals."""
    params = {"exchange": EXCHANGE, "limit": limit}
    if from_time_ms is not None:
        params["from_time"] = from_time_ms
    if to_time_ms is not None:
        params["to_time"] = to_time_ms
    headers, path = sign_request("GET", "/trade/api/v2/futures/transactions", params)
    r = requests.get(BASE_URL + path, headers=headers, timeout=15)
    if not r.ok:
        # raise_for_status() alone drops the response body, which is where
        # CoinSwitch actually explains *why* a 400 happened — surface it so
        # /fees's error message (and the logs) show the real reason instead
        # of a bare "400 Client Error: Bad Request for url: ...".
        raise RuntimeError(
            f"CoinSwitch Get Transactions failed: HTTP {r.status_code}, body: {r.text}"
        )
    return r.json()["data"]


def summarize_fees_and_pnl(from_time_ms=None, to_time_ms=None):
    """Buckets get_account_transactions() by type and returns:
        {"gross_pnl", "commission", "funding_fee", "liquidation_fee", "net_pnl"}
    all in USDT. commission/liquidation_fee come back negative (debits) per
    CoinSwitch's signed-amount convention, so net_pnl = the straight sum of
    all four — no separate subtraction needed. "Brokerage" in bot-speak
    means commission specifically (the per-trade taker fee); funding and
    liquidation fees are shown separately since they're a different thing
    (funding can even be a credit, not a cost) and lumping them into
    "brokerage" would be misleading.

    Individual bad/missing entries are skipped with a warning rather than
    raising, same as get_realized_pnl — one malformed row shouldn't blow up
    the whole /fees command."""
    totals = {"COMMISSION": 0.0, "FUNDING_FEE": 0.0, "P&L": 0.0, "LIQUIDATION_FEE": 0.0}
    for t in get_account_transactions(from_time_ms, to_time_ms):
        try:
            ttype = t["type"]
            amount = float(t["amount"])
        except (KeyError, ValueError, TypeError):
            print(f"  [fees] transaction entry missing/bad type or amount, skipping it. Raw: {t}")
            continue
        if ttype in totals:
            totals[ttype] += amount
    gross_pnl = totals["P&L"]
    commission = totals["COMMISSION"]
    funding_fee = totals["FUNDING_FEE"]
    liquidation_fee = totals["LIQUIDATION_FEE"]
    return {
        "gross_pnl": gross_pnl,
        "commission": commission,
        "funding_fee": funding_fee,
        "liquidation_fee": liquidation_fee,
        "net_pnl": gross_pnl + commission + funding_fee + liquidation_fee,
    }


def start_of_day_ist_ms(date_str):
    """Converts a 'YYYY-MM-DD' IST calendar date (as produced by today_ist())
    into that day's 00:00:00 IST instant, in Unix milliseconds — the
    from_time_ms boundary /fees uses for "today"."""
    y, m, d = (int(x) for x in date_str.split("-"))
    dt = datetime.datetime(y, m, d, 0, 0, 0, tzinfo=IST)
    return int(dt.timestamp() * 1000)


def get_wallet_balance(usdt_inr_rate=None, max_retries=2, retry_delay_seconds=2.0):
    """Returns the available futures wallet balance as a dict:
        {"total_usdt": float, "usdt_available": float,
         "inr_available": float, "inr_as_usdt": float}
    per CoinSwitch's Get Wallet Balance endpoint. "total_usdt" is the amount
    free to use for new orders/margin — callers that only care about the
    total (e.g. the pre-trade balance gate) should use result["total_usdt"];
    callers that want to show the breakdown (e.g. the Telegram status
    message) have the individual pieces too.

    CoinSwitch's futures wallet can hold BOTH a USDT balance and an INR
    balance under the same account (base_asset_balances returns one row per
    asset) — so money sitting as INR that hasn't been converted to USDT is
    just as usable for margin as USDT is. "total_usdt" adds both together,
    treating INR as USDT at the current live rate, so the bot doesn't sit
    idle (or wrongly report a zero balance) just because your funds happen
    to be parked as INR rather than USDT.

    usdt_inr_rate: pass the already-fetched live rate to avoid an extra
    network call; if omitted (e.g. called from a context that doesn't have
    it handy) this fetches it itself. If that fetch fails, the INR portion
    is skipped (with a warning) rather than failing the whole balance check.

    Raises requests.HTTPError on failure of the wallet call itself (caller
    decides how to handle a transient lookup failure). GET is idempotent,
    so — same reasoning as get_all_tickers() — this retries on both 429 and
    transient network errors, not just rate limiting."""
    headers, path = sign_request("GET", "/trade/api/v2/futures/wallet_balance")
    for attempt in range(max_retries + 1):
        try:
            r = requests.get(BASE_URL + path, headers=headers, timeout=15)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < max_retries:
                wait = retry_delay_seconds * (2 ** attempt)
                print(f"  [wallet] network error ({e}), retrying in {wait:.1f}s...")
                time.sleep(wait)
                continue
            raise
        if r.status_code == 429 and attempt < max_retries:
            wait = retry_delay_seconds * (2 ** attempt)
            print(f"  [wallet] rate-limited (429), retrying in {wait:.1f}s...")
            time.sleep(wait)
            continue
        r.raise_for_status()
        base_asset_balances = r.json()["data"]["base_asset_balances"]

        usdt_available = 0.0
        inr_available = 0.0
        found_any = False
        for entry in base_asset_balances:
            asset = entry.get("base_asset")
            if asset == "USDT":
                found_any = True
                usdt_available = float(entry["balances"]["total_available_balance"])
            elif asset == "INR":
                found_any = True
                inr_available = float(entry["balances"]["total_available_balance"])

        if not found_any:
            # No USDT or INR row at all — treat as zero available rather than
            # raising, so a single unexpected response shape doesn't crash
            # the whole scan cycle.
            print(f"  [wallet] no USDT or INR entry found in wallet balance response: {base_asset_balances}")
            return {"total_usdt": 0.0, "usdt_available": 0.0, "inr_available": 0.0, "inr_as_usdt": 0.0}

        inr_as_usdt = 0.0
        if inr_available > 0:
            rate = usdt_inr_rate
            if rate is None:
                try:
                    rate = get_usdt_inr_rate()
                except Exception as e:
                    print(f"  [wallet] have {inr_available:.2f} INR available but couldn't fetch "
                          f"USDT/INR rate to convert it ({e}) — counting only the USDT balance this cycle.")
                    rate = None
            if rate:
                inr_as_usdt = inr_available / rate

        total = usdt_available + inr_as_usdt
        if inr_available > 0:
            print(f"  [wallet] {usdt_available:.2f} USDT + {inr_available:.2f} INR "
                  f"(~{inr_as_usdt:.2f} USDT) = {total:.2f} USDT available.")
        return {
            "total_usdt": total,
            "usdt_available": usdt_available,
            "inr_available": inr_available,
            "inr_as_usdt": inr_as_usdt,
        }


def get_instrument_info():
    headers, path = sign_request(
        "GET", "/trade/api/v2/futures/instrument_info", {"exchange": EXCHANGE}
    )
    r = requests.get(BASE_URL + path, headers=headers, timeout=15)
    r.raise_for_status()
    return r.json()["data"]


def place_order(symbol, side, order_type, quantity, price=None,
                 trigger_price=None, reduce_only=False, max_retries=2, retry_delay_seconds=2.0):
    """POST is NOT idempotent — a retry after an ambiguous failure risks
    placing the same order twice, which is a much worse outcome than one
    missed cycle. So unlike the GET helpers above, this ONLY retries on 429
    (rate limited): a 429 response is a guarantee the order was rejected
    before ever reaching the matching engine, so retrying it is safe. A
    ConnectionError or Timeout, by contrast, means we genuinely don't know
    whether CoinSwitch received and processed the order before the
    connection dropped — those are deliberately NOT retried here and are
    left to propagate to the caller instead."""
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
    for attempt in range(max_retries + 1):
        r = requests.post(BASE_URL + path, headers=headers, json=body, timeout=15)
        if r.status_code == 429 and attempt < max_retries:
            wait = retry_delay_seconds * (2 ** attempt)
            print(f"  [order] rate-limited (429) placing order for {symbol}, retrying in {wait:.1f}s...")
            time.sleep(wait)
            continue
        if not r.ok:
            # raise_for_status() alone drops the response body, which is
            # where CoinSwitch actually explains *why* a 400 happened —
            # surface it so the logs/Telegram error show the real reason
            # (e.g. below-minimum notional, bad precision, invalid symbol)
            # instead of a bare "400 Client Error: Bad Request for url: ...".
            raise RuntimeError(
                f"CoinSwitch place order failed for {symbol} ({side} {quantity}): "
                f"HTTP {r.status_code}, body: {r.text}"
            )
        return r.json()


def cancel_order(symbol, order_id, max_retries=2, retry_delay_seconds=2.0):
    """Cancels a resting order (used to replace an existing TP or SL order
    when the user manually changes one via /tp or /sl).

    NOTE: same caveat as elsewhere in this file (see get_realized_pnl /
    recover_open_positions) — the exact cancel endpoint/method/body shape
    below is my best guess at CoinSwitch's REST convention (DELETE to the
    same /futures/order path, order_id + symbol + exchange in the body),
    NOT verified against their live API docs. Test this against a real
    (small) order before relying on it — if it 404s or the body shape is
    wrong, /tp and /sl will fail to replace the old order on a live
    position and you'll end up with two resting orders on the same
    symbol. Safe to ignore in DRY_RUN, which never reaches the network."""
    if DRY_RUN or order_id in (None, "DRY-RUN"):
        print(f"    [DRY RUN] would DELETE /futures/order -> {{'symbol': {symbol!r}, "
              f"'order_id': {order_id!r}}}")
        return {"data": {"order_id": order_id, "status": "DRY_RUN"}}

    body = {"exchange": EXCHANGE, "symbol": symbol, "order_id": order_id}
    headers, path = sign_request("DELETE", "/trade/api/v2/futures/order")
    for attempt in range(max_retries + 1):
        r = requests.delete(BASE_URL + path, headers=headers, json=body, timeout=15)
        if r.status_code == 429 and attempt < max_retries:
            wait = retry_delay_seconds * (2 ** attempt)
            print(f"  [order] rate-limited (429) cancelling order for {symbol}, retrying in {wait:.1f}s...")
            time.sleep(wait)
            continue
        if r.status_code == 404:
            # Already filled/cancelled — not an error for our purposes, the
            # caller just wants it gone before placing the replacement.
            print(f"  [order] cancel for {symbol} order {order_id} got 404 — "
                  f"already gone, treating as cancelled.")
            return {"data": {"order_id": order_id, "status": "ALREADY_GONE"}}
        r.raise_for_status()
        return r.json()

def screen_candidates(tickers, top_cap_symbols, usdt_inr_rate):
    """Apply rules 1-3: not top-200 cap, down >5% in 24h, 2cr-40cr INR volume."""
    candidates = []
    min_volume_usdt = MIN_24H_VOLUME_INR / usdt_inr_rate
    max_volume_usdt = MAX_24H_VOLUME_INR / usdt_inr_rate

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

        if (pct_change <= -MIN_24H_DROP_PCT
                and min_volume_usdt <= quote_volume <= max_volume_usdt):
            candidates.append({
                "symbol": symbol,
                "last_price": float(t["last_price"]),
                "pct_change_24h": pct_change,
                "quote_volume_24h_usdt": quote_volume,
            })

    return candidates


def screen_candidates_v2(tickers, top_cap_symbols):
    """Strategy 2's screener. Per instruction: ONLY the top-200-market-cap
    exclusion applies — no 24h-drop-% requirement and deliberately NO
    24h-volume check. Every remaining CoinSwitch futures symbol with a
    readable last_price is a candidate; the actual entry decision is made
    purely off 5m RSI in enter_trades_strategy2()."""
    candidates = []
    for symbol, t in tickers.items():
        base_symbol = symbol.replace("USDT", "").upper()
        if base_symbol in top_cap_symbols:
            continue
        try:
            last_price = float(t["last_price"])
        except (KeyError, ValueError):
            continue
        candidates.append({"symbol": symbol, "last_price": last_price})
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


def get_candle_volume(candle):
    """Best-effort extraction of a kline's traded volume. Same caveat as the
    other CoinSwitch field names guessed elsewhere in this file (see
    get_realized_pnl / recover_open_positions): the exact key hasn't been
    verified against a live response, so several common candidates are tried
    in order. Print a few raw candles the first time you run this if the
    declining-volume filter below ever looks like it's never confirming
    anything — that's the sign the real key isn't in this list."""
    for key in ("volume", "v", "base_asset_volume", "quote_asset_volume", "vol"):
        if key in candle:
            try:
                return float(candle[key])
            except (TypeError, ValueError):
                continue
    return None


def is_volume_declining(candles, lookback=VOLUME_DECLINE_LOOKBACK_CANDLES,
                         min_decline_pct=VOLUME_DECLINE_MIN_PCT):
    """Returns True/False/None:
      True  -> volume has clearly been shrinking over the most recent
               `lookback` closed 15m candles (buying pressure fading right as
               price pushes into the resistance level, rather than slamming
               into it on rising volume, which more often precedes a breakout
               rather than a rejection).
      False -> volume was readable and is NOT declining — a real reason to
               skip the trade.
      None  -> volume couldn't be read/confirmed at all (missing field, not
               enough candles yet, etc). This is treated as "unknown", not
               as a failure — see evaluate_resistance(), which proceeds as
               it did before this check existed rather than blocking every
               trade over a field-name mismatch."""
    window = candles[-lookback:]
    if len(window) < lookback:
        return None
    volumes = [get_candle_volume(c) for c in window]
    if any(v is None for v in volumes):
        print("  [volume] couldn't read a volume field off these candles — "
              "proceeding without the declining-volume check for this symbol.")
        return None
    mid = len(volumes) // 2
    first_half, second_half = volumes[:mid], volumes[mid:]
    avg_first = sum(first_half) / len(first_half)
    avg_second = sum(second_half) / len(second_half)
    if avg_first <= 0:
        return None
    decline_pct = (avg_first - avg_second) / avg_first * 100
    return decline_pct >= min_decline_pct


def evaluate_resistance(symbol, current_price, candles=None,
                         require_rejection_candle=None, require_declining_volume=None):
    """candles can be passed in (e.g. already fetched by the caller for the
    RSI check below) to avoid a second, redundant get_klines() call for the
    same symbol in the same scan cycle. If omitted, fetches them itself like
    before.

    require_rejection_candle / require_declining_volume default to the
    module-level REQUIRE_REJECTION_CANDLE / REQUIRE_DECLINING_VOLUME (i.e.
    strategy 1's behavior) when left as None. Strategy 3 passes its own
    STRATEGY3_REQUIRE_DECLINING_VOLUME and require_rejection_candle=False so
    it can short on a bare touch of the level without waiting for a
    confirmed rejection candle, while strategy 1's calls (which don't pass
    these) are completely unaffected."""
    if require_rejection_candle is None:
        require_rejection_candle = REQUIRE_REJECTION_CANDLE
    if require_declining_volume is None:
        require_declining_volume = REQUIRE_DECLINING_VOLUME
    if candles is None:
        candles = get_klines(symbol)
    if len(candles) < (2 * PIVOT_WING + 5):
        return None
    levels = find_resistance_levels(candles)
    hit = is_at_resistance(current_price, levels)
    if hit is None:
        return None
    if require_rejection_candle and not has_rejection_candle(candles):
        return None
    if require_declining_volume:
        # Only a confirmed False (volume readable and NOT declining) blocks
        # the trade. None (unreadable/insufficient data) falls through and
        # behaves like this check never existed, per your instruction not to
        # let a missing volume field silently kill every signal.
        if is_volume_declining(candles) is False:
            return None
    return hit


def get_strategy3_confirmed_resistance(symbol, candles):
    """Strategy 3's own resistance signal: a wick-rejection + confirmation,
    entirely off candle highs/closes (not the live ticker price the old
    bare-touch check used).

      Candle N:   does NOT close above a detected resistance level, but its
                  WICK (high) crosses above it -> a rejection candle. Arms a
                  pending confirmation for (symbol, level), returns None (no
                  entry yet).
      Candle N+1: the very next closed candle CLOSES BELOW that SAME level
                  -> returns the level (caller enters SHORT).
                  If it closes at/above the level instead, the pending
                  confirmation is discarded (candle N's rejection didn't
                  hold, so no short signal) — but that same candle N+1 can
                  still arm its own new pending confirmation if it itself
                  qualifies (see fallthrough below).

    "Very next candle" is enforced by checking that the previously-armed
    candle is exactly candles[-2] here, i.e. no candle was skipped in
    between (e.g. because this symbol dropped out of screening for a
    cycle or two) — a gap discards the stale pending confirmation instead
    of confirming off a non-adjacent candle.

    Only ever called from enter_trades_strategy3(); reads/writes the
    module-level strategy3_pending_confirmation dict."""
    if len(candles) < (2 * PIVOT_WING + 5):
        return None

    last_candle = candles[-1]
    last_ts = last_candle.get("start_time")
    last_close = float(last_candle["c"])

    pending = strategy3_pending_confirmation.get(symbol)

    if pending is not None:
        if pending["candle_ts"] == last_ts:
            # No new candle has closed since we armed — still waiting.
            return None
        prev_ts = candles[-2].get("start_time") if len(candles) >= 2 else None
        del strategy3_pending_confirmation[symbol]  # consumed either way below
        if prev_ts == pending["candle_ts"]:
            level = pending["level"]
            if last_close < level:
                print(f"  {symbol}: wick crossed resistance ~{level:.6g} while closing "
                      f"below it, then the very next 15m candle also closed below it "
                      f"— confirmed.")
                return level
            print(f"  {symbol}: wick crossed resistance ~{level:.6g} while closing "
                  f"below it, but the next 15m candle did NOT close below it — "
                  f"confirmation failed, not entering.")
        else:
            print(f"  {symbol}: gap in candle history since arming (symbol likely "
                  f"dropped out of screening for a cycle) — pending confirmation "
                  f"discarded rather than confirming off a non-adjacent candle.")
        # Fall through to check whether this same latest candle itself arms a
        # brand-new pending confirmation (e.g. it's its own wick-rejection).

    last_high = float(last_candle["h"])

    levels = find_resistance_levels(candles)
    for lvl in levels:
        # Candle must NOT close above the level (close stays below it), but
        # its WICK (high) must cross above the level — a rejection candle:
        # price poked through resistance intra-candle and got sold back
        # down before the close. Upper-bounded by the tolerance band on the
        # wick so a level the price wicked through long ago (and is now far
        # above) doesn't keep re-arming on every scan — mirrors the
        # tolerance is_at_resistance() uses for strategy 1.
        wick_crossed = lvl < last_high <= lvl * (1 + RESISTANCE_TOLERANCE_PCT / 100)
        closed_below = last_close < lvl
        if wick_crossed and closed_below:
            strategy3_pending_confirmation[symbol] = {"level": lvl, "candle_ts": last_ts}
            print(f"  {symbol}: 15m candle wick crossed resistance ~{lvl:.6g} but "
                  f"closed below it (~{last_close:.6g}) — waiting for the next "
                  f"candle to also close below it before entering short.")
            break
    return None


def compute_rsi(candles, period=RSI_PERIOD):
    """Standard Wilder RSI off closed 15m candle closes. Returns the RSI value
    (0-100) as of the most recent CLOSED candle, or None if there aren't
    enough candles yet to compute a `period`-length RSI.

    Uses the classic Wilder smoothing (first average = simple mean of the
    first `period` gains/losses, every value after that is an exponential
    smooth of the previous average), which is what "RSI" conventionally
    refers to on most charting platforms — not a plain rolling average."""
    closes = [float(c["c"]) for c in candles]
    if len(closes) < period + 1:
        return None

    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [d if d > 0 else 0.0 for d in deltas]
    losses = [-d if d < 0 else 0.0 for d in deltas]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def is_rsi_overbought_short_trigger(candles, threshold=RSI_OVERBOUGHT_SHORT_THRESHOLD, period=RSI_PERIOD):
    """Rule 4b (independent of resistance/rejection/volume-decline — rule 4):
    on the 15-minute chart, RSI(period) above `threshold` triggers a short on
    its own. Returns (triggered: bool, rsi_value: float|None). rsi_value is
    still returned when not triggered (or None) so callers can log/inspect
    it either way."""
    rsi_value = compute_rsi(candles, period)
    if rsi_value is None:
        return False, None
    return rsi_value > threshold, rsi_value


def compute_ema_series(candles, period):
    """Strategy 4's EMA9 helper. Returns a list the SAME LENGTH as `candles`,
    where index i is the EMA value computed using only closes[0..i] (seeded
    with a plain SMA over the first `period` closes at index period-1, the
    standard EMA convention, then the usual exponential recursion after
    that). Indices before the SMA seed has enough candles are None.

    This is deliberately a full aligned series, not just a single trailing
    number like compute_rsi() returns — Strategy 4 needs to compare EACH
    closed candle's own close against its OWN EMA9 value as of that same
    candle ("this candle closed above/below EMA9"), not just today's final
    EMA number against today's price."""
    closes = [float(c["c"]) for c in candles]
    n = len(closes)
    series = [None] * n
    if n < period:
        return series
    sma_seed = sum(closes[:period]) / period
    series[period - 1] = sma_seed
    multiplier = 2 / (period + 1)
    ema = sma_seed
    for i in range(period, n):
        ema = (closes[i] - ema) * multiplier + ema
        series[i] = ema
    return series


def compute_ema_cross_signal(candles, fast_period, slow_period):
    """Strategy 5's ("RE Strategy") EMA cross detector — ported directly from
    backtest_strategy_ema9_ema21_cross.py's compute_ema_cross_signal(),
    reusing compute_ema_series() for each EMA line instead of duplicating the
    EMA math. Returns a list the same length as `candles` where index i is
    'LONG', 'SHORT', or None.

    A crossover EVENT (not just "which EMA is currently bigger") fires on bar
    i only if EMA(fast) and EMA(slow) swap which one is bigger relative to
    bar i-1, AND bar i's own close confirms the same direction (closes above
    BOTH EMAs for a bullish cross -> LONG, below BOTH for a bearish cross ->
    SHORT). A crossover whose candle closes back through one of the EMAs (a
    whipsaw/doji-type bar) is dropped for that bar entirely — it does NOT
    wait around for a later candle to confirm; a fresh crossover is required
    to try again. This matches the backtest's replayed rules exactly, so
    forward behavior here should line up with what was backtested."""
    closes = [float(c["c"]) for c in candles]
    n = len(closes)
    ema_fast = compute_ema_series(candles, fast_period)
    ema_slow = compute_ema_series(candles, slow_period)

    signal_side = [None] * n
    prev_diff = None
    for i in range(n):
        if ema_fast[i] is None or ema_slow[i] is None:
            prev_diff = None
            continue
        diff = ema_fast[i] - ema_slow[i]
        if prev_diff is not None:
            crossed_up = prev_diff <= 0 < diff
            crossed_down = prev_diff >= 0 > diff
            if crossed_up and closes[i] > ema_fast[i] and closes[i] > ema_slow[i]:
                signal_side[i] = "LONG"
            elif crossed_down and closes[i] < ema_fast[i] and closes[i] < ema_slow[i]:
                signal_side[i] = "SHORT"
            # else: crossover happened but the candle closed back through one
            # of the EMAs (or exactly on it) — no signal, no deferral.
        prev_diff = diff
    return signal_side


def send_volume_debug(symbol):
    """Handles the /debugvolume SYMBOL command — fetches this symbol's most
    recent 15m candles and shows exactly what raw fields CoinSwitch returned
    for each one, alongside what get_candle_volume()/is_volume_declining()
    actually parsed out of them. Use this to confirm the real key CoinSwitch
    uses for kline volume instead of guessing from Railway logs — see
    get_candle_volume()'s candidate-key list if this shows "UNREADABLE" for
    every candle."""
    try:
        candles = get_klines(symbol, limit=max(VOLUME_DECLINE_LOOKBACK_CANDLES, 12))
    except Exception as e:
        send_telegram_message(f"⚠️ Couldn't fetch klines for {symbol}: {e}")
        return
    if not candles:
        send_telegram_message(
            f"No candles returned for {symbol} — check it's a valid CoinSwitch "
            f"futures symbol (e.g. DOGEUSDT)."
        )
        return

    lines = [f"🕵️ Volume debug for {symbol} — last {min(6, len(candles))} of {len(candles)} candle(s):"]
    for c in candles[-6:]:
        vol = get_candle_volume(c)
        vol_str = f"{vol:g}" if vol is not None else "UNREADABLE"
        # Dump every raw key CoinSwitch actually sent for this candle — not
        # just the ones this script currently expects — so a mismatched
        # volume field name (or anything else about the response shape) is
        # visible directly rather than something to guess at from logs.
        lines.append(f"{json.dumps(c, sort_keys=True)}\n  -> parsed volume: {vol_str}")

    verdict = is_volume_declining(candles)
    verdict_str = {
        True: "TRUE — confirmed declining, this check would pass a short here",
        False: "FALSE — readable but NOT declining, this check would block a short here",
        None: "UNKNOWN — couldn't read volume (or not enough candles yet); "
              "falls through and doesn't block, per your instruction",
    }[verdict]
    lines.append(f"\nis_volume_declining() verdict: {verdict_str}")

    msg = "\n\n".join(lines)
    # Telegram caps messages at 4096 chars; CoinSwitch candles can carry a
    # variable number of extra fields, so trim defensively rather than let
    # sendMessage silently fail on an oversized payload.
    if len(msg) > 3800:
        msg = msg[:3800] + "\n...[truncated — fewer candles or trim extra fields if you need the rest]"
    send_telegram_message(msg)


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
            "active_strategy": strategy_state.get("active", ACTIVE_STRATEGY_DEFAULT),
            "strategy3_pending_confirmation": strategy3_pending_confirmation,
            "saved_at_ms": int(time.time() * 1000),
        }
        tmp_path = STATE_FILE_PATH + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(payload, f)
        os.replace(tmp_path, STATE_FILE_PATH)  # atomic on POSIX — avoids a torn/partial
                                                # state file if the process dies mid-write
    except Exception as e:
        print(f"  [state] failed to save state file: {e}")


def restore_active_strategy_from_state():
    """Peeks at the saved state file for just the active-strategy field and,
    if present and valid, applies it to strategy_state immediately. This is
    called at the very top of main() — BEFORE the top-200 CoinGecko
    market-cap scan — specifically so that scan can be skipped on strategy
    4/5 deploys. It duplicates the same field load_state()/
    recover_open_positions() will read again later for open_shorts/
    daily_trade_tracker restoration; that second read is harmless and kept
    as-is so nothing else about startup ordering has to change. Never raises
    — a missing/corrupt file here just leaves strategy_state at its env-var
    default, exactly like load_state()'s existing fallback behavior."""
    try:
        with open(STATE_FILE_PATH, "r") as f:
            payload = json.load(f)
        saved_strategy = payload.get("active_strategy")
        if saved_strategy in ("1", "2", "3", "4", "5"):
            strategy_state["active"] = saved_strategy
    except Exception:
        pass  # fall back to the env-var default; load_state() will retry properly later


def load_state():
    """Returns (open_shorts, daily_trade_tracker) loaded from the local state
    file, or (None, None) if it doesn't exist or can't be parsed. A missing
    or corrupt file is NOT an error worth failing startup over — it just
    means recovery falls back to live-exchange-only reconstruction, same as
    before state persistence existed."""
    try:
        with open(STATE_FILE_PATH, "r") as f:
            payload = json.load(f)
        saved_strategy = payload.get("active_strategy")
        if saved_strategy in ("1", "2", "3", "4", "5"):
            strategy_state["active"] = saved_strategy
            print(f"  [state] restored active strategy from saved state: strategy {saved_strategy}")
        saved_pending = payload.get("strategy3_pending_confirmation")
        if saved_pending:
            strategy3_pending_confirmation.clear()
            strategy3_pending_confirmation.update(saved_pending)
            print(f"  [state] restored {len(saved_pending)} in-progress strategy 3 "
                  f"resistance confirmation(s) from saved state.")
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
        # Different calendar day — don't restore stale trade counts/P&L, but
        # the re-entry cooldown is a rolling window, not calendar-day
        # based, so it still needs to survive across the midnight rollover
        # (a symbol shorted at 23:50 IST yesterday should still be blocked
        # from re-entry this morning if within the cooldown window, not get
        # reset just because the date ticked over).
        daily_trade_tracker["recent_entries"] = state_daily_tracker.get("recent_entries") or {}
        daily_trade_tracker["recent_losses"] = state_daily_tracker.get("recent_losses") or {}
        daily_trade_tracker["recent_closes"] = state_daily_tracker.get("recent_closes") or {}
        print(f"  [state] saved state is from a previous day "
              f"({state_daily_tracker.get('date')}), not restoring today's counters "
              f"(re-entry, loss cooldown, and strategy-5 close cooldown timestamps were still restored).")
    daily_trade_tracker.setdefault("recent_entries", {})
    daily_trade_tracker.setdefault("recent_losses", {})
    daily_trade_tracker.setdefault("recent_closes", {})

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
        # Backfill for state saved before strategy 2 (long support) existed —
        # every position tracked back then was strategy 1's SHORT-only, so
        # that's the correct default, not a guess.
        pos.setdefault("side", "SHORT")
        pos.setdefault("strategy", "1")
        # Backfill for state saved before /tp and /sl (manual TP/SL editing)
        # existed — no stop-loss and no known TP order id for these older
        # positions.
        pos.setdefault("tp_order_id", None)
        pos.setdefault("sl_price", None)
        pos.setdefault("sl_order_id", None)
        pos.setdefault("price_precision", 4)
    if recovered:
        print(f"  [state] restored {len(recovered)} simulated (DRY RUN) open short(s) "
              f"from saved state: {', '.join(recovered.keys())}")

    symbols_to_check_desc_suffix = ""
    active_strategy_for_recovery = strategy_state.get("active", ACTIVE_STRATEGY_DEFAULT)
    if active_strategy_for_recovery in ("4", "5"):
        # Strategy 4/5 only ever open new trades on a small fixed symbol
        # list (see enter_trades_strategy4/5()) — checking all ~670+
        # CoinSwitch futures symbols one-by-one at the 3.1s/symbol pace
        # below (CoinSwitch's Get Positions rate limit) can take ~35
        # minutes on every single deploy for no benefit, since a position
        # this bot itself would open can only ever land on one of these
        # symbols. Still also check every symbol saved state remembers a
        # REAL (non-simulated) position on, regardless of which strategy
        # opened it or whether it's still in this strategy's own list —
        # a position doesn't stop existing just because the active
        # strategy changed since it was opened.
        strategy_fixed_symbols = (
            {STRATEGY4_SYMBOL} if active_strategy_for_recovery == "4" else set(STRATEGY5_SYMBOLS)
        )
        saved_real_symbols = {
            s for s, pos in state_open_shorts.items() if not pos.get("simulated")
        }
        symbols = sorted(
            (strategy_fixed_symbols | saved_real_symbols) & set(instruments.keys())
        )
        symbols_to_check_desc_suffix = (
            f" (strategy {active_strategy_for_recovery}'s fixed symbol(s) plus any symbol with a "
            f"saved real position — not the full {len(instruments)}-symbol market, since strategy "
            f"{active_strategy_for_recovery} never opens trades outside its fixed list)"
        )
    else:
        # Strategy 1/2/3 screen candidates from across the whole market (see
        # run_once()), so a leftover real position genuinely could be on any
        # symbol — the full scan is actually needed here.
        symbols = list(instruments.keys())
    print(f"Checking {len(symbols)} symbols on CoinSwitch for pre-existing open "
          f"positions{symbols_to_check_desc_suffix}...")
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
            # None. Strategy 2 can open LONGs as well as SHORTs, so both
            # directions are now tracked (side is recorded below and every
            # downstream P&L/liquidation/close calculation branches on it) —
            # previously anything not SHORT was assumed to be a stray manual
            # position and ignored; that's no longer a safe assumption now
            # that this bot itself opens longs too.
            p = positions[0]
            live_side = p.get("position_side") or "SHORT"
            if live_side not in ("SHORT", "LONG"):
                print(f"  [recover] {symbol}: open position has an unrecognized position_side "
                      f"({live_side!r}) — not tracking it as one of this bot's trades. Raw: {p}")
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
                tp_order_id = saved.get("tp_order_id")
                sl_price = saved.get("sl_price")
                sl_order_id = saved.get("sl_order_id")
                price_precision = saved.get("price_precision", 4)
                opened_at_ms = saved.get("opened_at_ms", int(time.time() * 1000))
                liquidation_warning_sent = saved.get("liquidation_warning_sent", False)
                # Saved state (if any) knows which strategy actually opened
                # this position; trust that over guessing. If there's no
                # matching saved entry (e.g. state file lost), fall back to
                # inferring from direction — but this is now ambiguous for
                # SHORTs, since both strategy 2 (RSI>80) and strategy 3
                # (resistance touch) open SHORTs. LONG still narrows cleanly
                # to strategy 1 (bug: opens LONG despite the short setup) or
                # strategy 2 (RSI<20). Defaulting a directionless-tiebreak
                # SHORT to "2" here; this only affects LIVE (non-simulated)
                # positions recovered with no matching state-file entry — a
                # rare edge case, not the normal path.
                strategy = saved.get("strategy") or "2"
            else:
                tp_price = None
                tp_order_id = None
                sl_price = None
                sl_order_id = None
                price_precision = 4
                opened_at_ms = int(time.time() * 1000)  # true entry time unknown otherwise
                liquidation_warning_sent = False
                strategy = "2"  # directionless tiebreak, same rationale as above

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
                "tp_order_id": tp_order_id,
                "sl_price": sl_price,
                "sl_order_id": sl_order_id,
                "price_precision": price_precision,
                "opened_at_ms": opened_at_ms,
                "simulated": False,           # always a real exchange position, regardless of today's DRY_RUN setting
                "leverage": leverage,
                "liquidation_warning_sent": liquidation_warning_sent,
                "side": live_side,
                "strategy": strategy,
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

def record_loss_cooldown(symbol, pnl, daily_trade_tracker, closed_at_ms):
    """Stamps symbol -> closed_at_ms in daily_trade_tracker["recent_losses"]
    whenever a trade closes at a loss, so the entry functions can enforce
    LOSS_COOLDOWN_MS before allowing a re-entry on that symbol. A later win
    on the same symbol does NOT clear this — the cooldown is purely time-based,
    same as recent_entries, so it always expires after LOSS_COOLDOWN_HOURS
    rather than being reset by whatever closes next. Call this from every
    close path (reconcile, manual close) alongside record_trade_close()."""
    if pnl < 0:
        daily_trade_tracker.setdefault("recent_losses", {})[symbol] = closed_at_ms


def record_recent_close(symbol, daily_trade_tracker, closed_at_ms):
    """Stamps symbol -> closed_at_ms in daily_trade_tracker["recent_closes"]
    on EVERY close, win or loss alike — used by strategy 5's own
    STRATEGY5_REENTRY_COOLDOWN_HOURS gate (see enter_trades_strategy5()) to
    stop it from re-entering the same symbol within an hour of that symbol's
    last close. Unlike record_loss_cooldown(), this doesn't check pnl at
    all. Call this from every close path alongside record_loss_cooldown()
    and record_trade_close()."""
    daily_trade_tracker.setdefault("recent_closes", {})[symbol] = closed_at_ms


def record_trade_close(symbol, pos, pnl, reason):
    """Appends one row to the trade-history CSV for every position that
    closes, however it closed (take-profit, manual Telegram button, or
    detected already-closed on the exchange). Best-effort — a logging
    failure here should never block or fail the actual close."""
    try:
        file_exists = os.path.exists(TRADE_HISTORY_FILE_PATH)
        with open(TRADE_HISTORY_FILE_PATH, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow([
                    "closed_at_ist", "symbol", "entry_price", "qty", "leverage",
                    "pnl_usdt", "simulated", "reason", "opened_at_ist",
                ])
            opened_at_ms = pos.get("opened_at_ms")
            opened_at_ist = (
                datetime.datetime.fromtimestamp(opened_at_ms / 1000, IST).strftime("%Y-%m-%d %H:%M:%S")
                if opened_at_ms else ""
            )
            writer.writerow([
                datetime.datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
                symbol,
                pos.get("entry_price", ""),
                pos.get("qty", ""),
                pos.get("leverage", ""),
                f"{pnl:.4f}",
                pos.get("simulated", DRY_RUN),
                reason,
                opened_at_ist,
            ])
    except Exception as e:
        print(f"  [trade history] failed to log closed trade for {symbol}: {e}")


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
            # We simulate the two exits this bot can produce for a DRY_RUN
            # position — the take-profit limit, and (if manually set via
            # /sl) a stop-loss — by checking whether the live price has
            # reached either one. With no SL set, a DRY RUN short can
            # otherwise stay open forever.
            t = tickers.get(symbol)
            if t is None:
                continue
            try:
                last_price = float(t["last_price"])
            except (KeyError, ValueError):
                continue
            side = pos.get("side", "SHORT")
            sl_price = pos.get("sl_price")
            if sl_price is not None:
                sl_hit = (
                    last_price >= sl_price if side == "SHORT"
                    else last_price <= sl_price
                )
                if sl_hit:
                    pnl = (
                        (pos["entry_price"] - sl_price) * pos["qty"] if side == "SHORT"
                        else (sl_price - pos["entry_price"]) * pos["qty"]
                    )
                    closed.append((symbol, pnl, pos, "stop_loss"))
                    continue  # don't also check TP this cycle — one exit per position
            if pos["tp_price"] is not None:
                tp_hit = (
                    last_price <= pos["tp_price"] if side == "SHORT"
                    else last_price >= pos["tp_price"]
                )
                if tp_hit:
                    pnl = (
                        (pos["entry_price"] - pos["tp_price"]) * pos["qty"] if side == "SHORT"
                        else (pos["tp_price"] - pos["entry_price"]) * pos["qty"]
                    )
                    closed.append((symbol, pnl, pos, "take_profit"))
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
            closed.append((symbol, pnl, pos, "exchange_closed"))

    for symbol, pnl, pos, reason in closed:
        del open_shorts[symbol]
        daily_trade_tracker["realized_pnl_usdt"] += pnl
        daily_trade_tracker["trades_closed"] += 1
        if pnl >= 0:
            daily_trade_tracker["wins"] += 1
        else:
            daily_trade_tracker["losses"] += 1
        record_loss_cooldown(symbol, pnl, daily_trade_tracker, int(time.time() * 1000))
        record_recent_close(symbol, daily_trade_tracker, int(time.time() * 1000))
        record_trade_close(symbol, pos, pnl, reason)
        print(f"  [reconcile] {symbol}: position closed. P&L {pnl:+.2f} USDT.")
        if reason == "exchange_closed" and not pos.get("simulated", DRY_RUN):
            # CORE BUG FIX (same issue as close_position_manual()): this
            # branch fires whenever get_positions() finds the symbol flat
            # again, which happens whether the TP filled, the SL filled, OR
            # the position was closed some other way entirely (manually in
            # the CoinSwitch app, liquidated, etc) — reconcile_open_shorts()
            # doesn't know which. Either way, whichever of TP/SL DIDN'T
            # cause the close is left resting on the exchange with nothing
            # to protect anymore. Cancel both defensively; cancel_order()
            # already treats "already gone" (404, e.g. the one that DID
            # trigger) as a safe no-op, so this can't fail on the
            # already-filled order.
            for order_id in (pos.get("tp_order_id"), pos.get("sl_order_id")):
                if order_id:
                    try:
                        cancel_order(symbol, order_id)
                    except Exception as e:
                        print(f"  [reconcile] {symbol}: failed to cancel leftover order {order_id} "
                              f"({e}) — it may still be resting on the exchange, check manually.")
        send_telegram_message(
            f"{'[DRY RUN] ' if DRY_RUN else ''}{symbol} position closed. P&L: {pnl:+.2f} USDT"
        )

    if closed:
        save_state(open_shorts, daily_trade_tracker)


def check_strategy4_signal_exits(open_shorts, daily_trade_tracker):
    """Runs every cycle, regardless of which strategy is currently ACTIVE for
    NEW entries (same reasoning as reconcile_open_shorts/check_liquidation_
    warnings — an open position keeps being monitored no matter what you
    switch the active strategy to). If there's an open Strategy 4 position on
    STRATEGY4_SYMBOL, checks whether the latest CLOSED 15m candle has closed
    back across EMA9 against that position's direction:
        LONG open  + latest close < EMA9 -> close (signal reversed)
        SHORT open + latest close > EMA9 -> close (signal reversed)
    This is the SECOND way a Strategy 4 position can close — the first being
    its resting take-profit limit order (STRATEGY4_TP_PRICE_MOVE_PCT), which
    reconcile_open_shorts() (called right before this, same cycle) already
    catches if it filled. Whichever happens first wins; if reconcile already
    removed the position this cycle (TP hit), open_shorts.get() below simply
    returns None and this is a no-op.

    Caller (run_once()) MUST already hold state_lock, same as
    reconcile_open_shorts() — this reads-then-mutates open_shorts /
    daily_trade_tracker, the same state a manual Telegram close touches."""
    pos = open_shorts.get(STRATEGY4_SYMBOL)
    if pos is None or pos.get("strategy") != "4":
        return

    try:
        candles = get_klines(STRATEGY4_SYMBOL, interval=STRATEGY4_KLINE_INTERVAL,
                              limit=STRATEGY4_LOOKBACK_CANDLES)
    except Exception as e:
        print(f"  [strategy4 exit] {STRATEGY4_SYMBOL}: klines fetch failed ({e}), "
              f"leaving position open this cycle.")
        return
    if not candles:
        return

    ema_series = compute_ema_series(candles, STRATEGY4_EMA_PERIOD)
    if ema_series[-1] is None:
        return

    latest_close = float(candles[-1]["c"])
    latest_ema = ema_series[-1]
    side = pos.get("side", "LONG")

    should_exit = (
        (side == "LONG" and latest_close < latest_ema) or
        (side == "SHORT" and latest_close > latest_ema)
    )
    if not should_exit:
        return

    print(f"  [strategy4 exit] {STRATEGY4_SYMBOL}: latest 15m close {latest_close:.6g} is back "
          f"across EMA9 ({latest_ema:.6g}) against the open {side} — closing on signal reversal.")

    is_simulated = pos.get("simulated", DRY_RUN)
    qty = pos.get("qty")
    entry_price = pos.get("entry_price")
    close_side = "SELL" if side == "LONG" else "BUY"

    # Cancel the resting take-profit order first (real trades only) so it
    # can't sit there and unexpectedly fill/interact after we've already
    # market-closed the position out from under it.
    if not is_simulated and pos.get("tp_order_id"):
        try:
            cancel_order(STRATEGY4_SYMBOL, pos["tp_order_id"])
        except Exception as e:
            print(f"  [strategy4 exit] {STRATEGY4_SYMBOL}: failed to cancel resting TP order "
                  f"({e}), continuing with the market close anyway.")

    if is_simulated:
        # Nothing real was ever placed, so there's nothing to send a close
        # order for — just book the exit at this candle's close price.
        if entry_price is not None and qty is not None:
            pnl = (
                (latest_close - entry_price) * qty if side == "LONG"
                else (entry_price - latest_close) * qty
            )
        else:
            pnl = 0.0
        print(f"  [strategy4 exit] {STRATEGY4_SYMBOL}: [DRY RUN] closing simulated {side} "
              f"position, est P&L {pnl:+.2f} USDT.")
    else:
        try:
            resp = place_order(STRATEGY4_SYMBOL, side=close_side, order_type="MARKET",
                                quantity=qty, reduce_only=True)
            print(f"  [strategy4 exit] {STRATEGY4_SYMBOL}: close order placed -> {resp['data']}")
        except Exception as e:
            print(f"  [strategy4 exit] {STRATEGY4_SYMBOL}: failed to place close order ({e}), "
                  f"leaving position tracked as open.")
            send_telegram_message(f"⚠️ Strategy 4 signal-exit close failed for {STRATEGY4_SYMBOL}: {e}")
            return
        time.sleep(2)  # give CoinSwitch a moment to settle the fill before pulling realized P&L
        try:
            pnl = get_realized_pnl(STRATEGY4_SYMBOL, pos["opened_at_ms"])
        except Exception as e:
            print(f"  [strategy4 exit] {STRATEGY4_SYMBOL}: P&L lookup failed ({e}), "
                  f"closing with unknown P&L.")
            pnl = 0.0

    del open_shorts[STRATEGY4_SYMBOL]
    daily_trade_tracker["realized_pnl_usdt"] += pnl
    daily_trade_tracker["trades_closed"] += 1
    if pnl >= 0:
        daily_trade_tracker["wins"] += 1
    else:
        daily_trade_tracker["losses"] += 1
    record_loss_cooldown(STRATEGY4_SYMBOL, pnl, daily_trade_tracker, int(time.time() * 1000))
    record_recent_close(STRATEGY4_SYMBOL, daily_trade_tracker, int(time.time() * 1000))
    record_trade_close(STRATEGY4_SYMBOL, pos, pnl, "strategy4_ema9_signal_exit")
    save_state(open_shorts, daily_trade_tracker)
    send_telegram_message(
        f"{'[DRY RUN] ' if is_simulated else ''}[Strategy 4] {STRATEGY4_SYMBOL} closed on EMA9 "
        f"signal reversal ({side} exited). P&L: {pnl:+.2f} USDT"
    )


# ------------------------------ Position monitoring ------------------------------

def estimate_liquidation_price(entry_price, leverage, side="SHORT"):
    """Rough isolated-margin liquidation price estimate, ignoring maintenance
    margin rate, funding, and fees — none of which this script fetches from
    CoinSwitch. A position's margin (entry_price*qty/leverage) is fully
    wiped once price has moved against it by entry_price/leverage, so:

        SHORT: liq_price ~= entry_price * (1 + 1/leverage)   (price UP wipes it)
        LONG:  liq_price ~= entry_price * (1 - 1/leverage)   (price DOWN wipes it)

    In reality the exchange liquidates earlier than this once losses eat into
    the maintenance margin buffer, so treat this as an optimistic upper bound
    on how much room the position actually has — the real liquidation price
    is always somewhat closer (in adverse-move terms) than this estimate.
    Good enough for an early-warning Telegram alert; not something to rely on
    for precise risk sizing."""
    if not leverage or leverage <= 0:
        return None
    if side == "LONG":
        return entry_price * (1 - 1.0 / leverage)
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

        liq_price = estimate_liquidation_price(entry_price, leverage, pos.get("side", "SHORT"))
        if liq_price is None:
            continue
        side = pos.get("side", "SHORT")
        if side == "SHORT":
            if liq_price <= entry_price:
                continue
            distance_covered_pct = (current_price - entry_price) / (liq_price - entry_price) * 100
        else:
            if liq_price >= entry_price:
                continue
            distance_covered_pct = (entry_price - current_price) / (entry_price - liq_price) * 100

        already_sent = pos.get("liquidation_warning_sent", False)
        if distance_covered_pct >= LIQUIDATION_WARNING_PCT:
            if not already_sent:
                print(f"  [liquidation] {symbol}: {distance_covered_pct:.0f}% of the way to "
                      f"estimated liquidation ({liq_price:.6g}) — sending warning.")
                send_telegram_message(
                    f"⚠️ {'[DRY RUN] ' if pos.get('simulated') else ''}{symbol} {side} is "
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


def check_loss_warnings(open_shorts, tickers):
    """Sends a one-time Telegram alert per position the first cycle its
    unrealized loss reaches LOSS_ALERT_PCT of the margin put up for that
    trade (i.e. price-move % against the position, times leverage — how much
    of the capital actually risked on this trade is currently under water).
    Mirrors check_liquidation_warnings()'s re-arm behaviour: if the position
    recovers back under the threshold it clears the flag, so a position that
    dips past -30%, recovers, and dips again later gets alerted both times
    instead of going silent for the rest of its life."""
    changed = False
    for symbol, pos in open_shorts.items():
        entry_price = pos.get("entry_price")
        qty = pos.get("qty")
        leverage = pos.get("leverage")
        if entry_price is None or qty is None or not leverage:
            continue  # can't compute a margin-relative loss % without all three

        t = tickers.get(symbol)
        if t is None:
            continue
        try:
            current_price = float(t["last_price"])
        except (KeyError, ValueError):
            continue

        side = pos.get("side", "SHORT")
        # SHORT: profit when price falls, loss when price rises above entry.
        # LONG: profit when price rises, loss when price falls below entry.
        unrealized = (
            (entry_price - current_price) * qty if side == "SHORT"
            else (current_price - entry_price) * qty
        )
        if unrealized >= 0:
            # In profit (or flat) — make sure the flag is re-armed and move on.
            if pos.get("loss_warning_sent", False):
                pos["loss_warning_sent"] = False
                changed = True
            continue

        margin = (entry_price * qty) / leverage
        if margin <= 0:
            continue
        loss_pct = -unrealized / margin * 100  # positive number = % of margin lost

        already_sent = pos.get("loss_warning_sent", False)
        if loss_pct >= LOSS_ALERT_PCT:
            if not already_sent:
                print(f"  [loss alert] {symbol}: unrealized loss is {loss_pct:.1f}% of margin "
                      f"({unrealized:+.2f} USDT) — sending warning.")
                send_telegram_message(
                    f"🔻 {'[DRY RUN] ' if pos.get('simulated') else ''}{symbol} {side} is down "
                    f"{loss_pct:.1f}% of margin ({unrealized:+.2f} USDT).\n"
                    f"Entry: {entry_price}  |  Current: {current_price}  |  Leverage: {leverage}x"
                )
                pos["loss_warning_sent"] = True
                changed = True
        elif already_sent:
            # Recovered back above the threshold — re-arm so a future dip
            # alerts again instead of staying permanently silenced.
            pos["loss_warning_sent"] = False
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
                side = pos.get("side", "SHORT")
                strategy = pos.get("strategy", "1")
                # SHORT: profit when price has fallen below entry.
                # LONG: profit when price has risen above entry.
                if side == "SHORT":
                    unrealized = (entry_price - current_price) * qty
                    pct_move = (entry_price - current_price) / entry_price * 100
                else:
                    unrealized = (current_price - entry_price) * qty
                    pct_move = (current_price - entry_price) / entry_price * 100
                emoji = "🟢" if unrealized > 0 else ("🔴" if unrealized < 0 else "⚪")
                total_unrealized += unrealized
                priced_count += 1
                tp_price = pos.get("tp_price")
                sl_price = pos.get("sl_price")
                tp_sl_desc = (
                    f"  TP {tp_price if tp_price is not None else '—'} / "
                    f"SL {sl_price if sl_price is not None else '—'}"
                )
                lines.append(
                    f"{emoji} {symbol} [{side}/S{strategy}]{' [DRY RUN]' if pos.get('simulated') else ''}: "
                    f"{unrealized:+.2f} USDT ({pct_move:+.2f}% price move)  "
                    f"entry {entry_price} -> now {current_price}{tp_sl_desc}"
                )
                # Quick-tap TP/SL buttons, flat % price-move off entry (see
                # QUICK_TPSL_PCTS) — one row for TP, one for SL, both above
                # the Close button below. Only shown when entry_price is
                # known, since the target price is computed off it.
                keyboard_rows.append([
                    {"text": f"🎯 TP {pct}%", "callback_data": f"tppct:{symbol}:{pct}"}
                    for pct in QUICK_TPSL_PCTS
                ])
                keyboard_rows.append([
                    {"text": f"🛑 SL {pct}%", "callback_data": f"slpct:{symbol}:{pct}"}
                    for pct in QUICK_TPSL_PCTS
                ])
                # Telegram buttons can't collect free-text input, so "Custom %"
                # just prompts you to reply with the /tppct or /slpct text
                # command (see apply_percent_tp_sl()), which accepts ANY
                # percentage, not just the three quick presets above.
                keyboard_rows.append([
                    {"text": "✏️ Custom TP %", "callback_data": f"customhelp:tp:{symbol}"},
                    {"text": "✏️ Custom SL %", "callback_data": f"customhelp:sl:{symbol}"},
                ])
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
        wallet = get_wallet_balance()
        msg += f"\n\n💰 Wallet balance (free): {wallet['total_usdt']:.2f} USDT"
        if wallet["inr_available"] > 0:
            msg += (f"\n   ({wallet['usdt_available']:.2f} USDT + "
                     f"{wallet['inr_available']:.2f} INR ≈ {wallet['inr_as_usdt']:.2f} USDT)")
    except Exception as e:
        print(f"  [status update] wallet balance lookup failed: {e}")
        msg += "\n\n💰 Wallet balance: unavailable this cycle"

    if bot_paused.is_set():
        msg += "\n\n⏸ New entries paused (/resume to re-enable)"

    print(f"\n[status update] {msg}")
    send_telegram_message(msg, reply_markup={"inline_keyboard": keyboard_rows} if keyboard_rows else None)


def send_daily_summary(daily_trade_tracker, open_shorts):
    pnl = daily_trade_tracker["realized_pnl_usdt"]
    emoji = "🟢" if pnl > 0 else ("🔴" if pnl < 0 else "⚪")

    fees_line = ""
    try:
        fees = summarize_fees_and_pnl(from_time_ms=start_of_day_ist_ms(daily_trade_tracker["date"]))
        fees_line = (
            f"Brokerage paid: {-fees['commission']:.2f} USDT\n"
            f"Net P&L after fees: {fees['net_pnl']:+.2f} USDT\n"
        )
    except Exception as e:
        print(f"  [daily summary] couldn't fetch fee breakdown ({e}), omitting it from today's summary.")
        fees_line = f"⚠️ Fee/P&L breakdown unavailable ({e})\n"

    msg = (
        f"{emoji} {'[DRY RUN] ' if DRY_RUN else ''}Daily summary — {daily_trade_tracker['date']}\n"
        f"Trades opened: {daily_trade_tracker['count']}\n"
        f"Trades closed: {daily_trade_tracker['trades_closed']} "
        f"(W {daily_trade_tracker['wins']} / L {daily_trade_tracker['losses']})\n"
        f"Realized P&L: {pnl:+.2f} USDT\n"
        f"{fees_line}"
        f"Still open (carrying into today): {len(open_shorts)}"
    )
    print(f"\n[daily summary] {msg}")
    send_telegram_message(msg)


def backup_trade_history():
    """Sends TRADE_HISTORY_FILE_PATH to Telegram as a document — the same
    thing the /history command does on demand, but run automatically once a
    day at the midnight-IST rollover. Railway's filesystem is ephemeral (a
    redeploy or restart can wipe it), so this is what actually makes the
    trade history durable: it lands in Telegram's chat history, off-Railway,
    once a day, rather than only existing as a local CSV that a bad restart
    could lose entirely. Best-effort — a failed backup shouldn't crash the
    daily-rollover cycle that calls it."""
    if not os.path.exists(TRADE_HISTORY_FILE_PATH):
        print("  [backup] no trade history file yet — nothing to back up today.")
        return
    try:
        send_telegram_document(
            TRADE_HISTORY_FILE_PATH,
            caption=f"📦 Daily trade history backup — {today_ist()}"
        )
        print(f"  [backup] sent {TRADE_HISTORY_FILE_PATH} to Telegram.")
    except Exception as e:
        print(f"  [backup] failed to send trade history backup: {e}")


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
    side = pos.get("side", "SHORT")
    close_side = "BUY" if side == "SHORT" else "SELL"  # closing a SHORT buys it back, closing a LONG sells it
    pnl_is_estimate = False  # set True below if get_realized_pnl() came back empty and we had to fall back

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
            pnl = (
                (entry_price - last_price) * qty if side == "SHORT"
                else (last_price - entry_price) * qty
            )
        else:
            pnl = 0.0
        print(f"  [manual close] {symbol}: [DRY RUN] closing simulated {side} position, est P&L {pnl:+.2f} USDT.")
    else:
        try:
            resp = place_order(symbol, side=close_side, order_type="MARKET", quantity=qty, reduce_only=True)
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
        pnl_is_estimate = False
        try:
            pnl = get_realized_pnl(symbol, pos["opened_at_ms"])
            if pnl == 0.0:
                # A real fill netting to EXACTLY $0.00 is possible but rare —
                # far more likely CoinSwitch's P&L transaction for this fill
                # hadn't posted yet 2s after the close order, or (same
                # unverified-field-name caveat as get_positions()) the
                # 'amount'/'type' fields don't match what get_realized_pnl()
                # expects and every entry silently got skipped/filtered to
                # nothing. One retry after a longer wait, then fall back to
                # a price-based estimate rather than reporting a possibly-
                # fictitious "+0.00 USDT" — seen live on a DEEPUSDT close
                # that the wallet balance confirmed was actually profitable.
                time.sleep(3)
                pnl = get_realized_pnl(symbol, pos["opened_at_ms"])
        except Exception as e:
            print(f"  [manual close] {symbol}: P&L lookup failed ({e}), will estimate from price instead.")
            pnl = 0.0

        if pnl == 0.0:
            last_price = None
            try:
                tickers = get_all_tickers()
                t = tickers.get(symbol)
                if t is not None:
                    last_price = float(t["last_price"])
            except Exception as e:
                print(f"  [manual close] {symbol}: couldn't fetch price for P&L estimate fallback ({e}).")
            if entry_price is not None and last_price is not None and qty is not None:
                pnl = (
                    (entry_price - last_price) * qty if side == "SHORT"
                    else (last_price - entry_price) * qty
                )
                pnl_is_estimate = True
                print(f"  [manual close] {symbol}: get_realized_pnl() returned 0.00 — "
                      f"falling back to price-based estimate {pnl:+.2f} USDT (fees excluded).")

        # CORE BUG FIX: the TP and SL orders placed at entry are two
        # independent resting orders on the exchange, not linked to each
        # other. Closing the position here (via a fresh reduce_only MARKET
        # order) does NOT automatically remove whichever of the two didn't
        # trigger — it stays resting on CoinSwitch. If price later swings
        # back across that stale trigger, there's a real risk it fires
        # anyway (reduce_only enforcement on a STOP_MARKET trigger isn't
        # something to bet the account on) and opens a brand-new position
        # this bot has zero record of — untracked, unprotected, and
        # invisible to /status, silently costing margin and fees with no
        # explanation. Best-effort, never blocks the close that already
        # happened: cancel_order() already treats "already gone" (404) as
        # success, so this is a safe no-op if the order already
        # filled/expired/was the one that triggered this close.
        for order_id in (pos.get("tp_order_id"), pos.get("sl_order_id")):
            if order_id:
                try:
                    cancel_order(symbol, order_id)
                except Exception as e:
                    print(f"  [manual close] {symbol}: failed to cancel leftover order {order_id} "
                          f"({e}) — it may still be resting on the exchange, check manually.")

    del open_shorts[symbol]
    daily_trade_tracker["realized_pnl_usdt"] += pnl
    daily_trade_tracker["trades_closed"] += 1
    if pnl >= 0:
        daily_trade_tracker["wins"] += 1
    else:
        daily_trade_tracker["losses"] += 1
    record_loss_cooldown(symbol, pnl, daily_trade_tracker, int(time.time() * 1000))
    record_recent_close(symbol, daily_trade_tracker, int(time.time() * 1000))
    record_trade_close(symbol, pos, pnl, "manual_telegram")
    save_state(open_shorts, daily_trade_tracker)

    send_telegram_message(
        f"✅ {'[DRY RUN] ' if is_simulated else ''}{symbol} manually closed. P&L: {pnl:+.2f} USDT"
        f"{' (estimated, fees excluded — CoinSwitch P&L data unavailable)' if pnl_is_estimate else ''}"
    )


def set_take_profit_manual(symbol, open_shorts, daily_trade_tracker, new_tp_price):
    """Handles /tp SYMBOL PRICE — replaces the resting take-profit order
    (live) or just updates the tracked tp_price (DRY_RUN — checked against
    the live price every cycle in reconcile_open_shorts()) for one open
    position. Caller (telegram_polling_loop) MUST already hold state_lock,
    same requirement as close_position_manual().

    A price of 0 (or any non-positive number) removes the take-profit
    entirely — cancels the resting order (live) and leaves the position
    with no automatic profit exit, same as TP_CAPITAL_PCT=0 would at entry.
    Returns (ok: bool, message: str) — caller sends the message to Telegram."""
    pos = open_shorts.get(symbol)
    if pos is None:
        return False, f"⚠️ No open position found for {symbol} (already closed?)."

    side = pos.get("side", "SHORT")
    entry_price = pos.get("entry_price")
    is_simulated = pos.get("simulated", DRY_RUN)

    if new_tp_price <= 0:
        old_order_id = pos.get("tp_order_id")
        if old_order_id and not is_simulated:
            try:
                cancel_order(symbol, old_order_id)
            except Exception as e:
                return False, f"⚠️ Failed to cancel existing TP order for {symbol}: {e}"
        pos["tp_price"] = None
        pos["tp_order_id"] = None
        save_state(open_shorts, daily_trade_tracker)
        return True, f"✅ Take-profit removed for {symbol}. No automatic profit exit is set now."

    # Sanity check: a take-profit belongs on the profit side of entry, not
    # the loss side — catches an obvious fat-finger (e.g. price on the wrong
    # side, or SL/TP swapped) before it does something surprising.
    if entry_price is not None:
        if side == "SHORT" and new_tp_price >= entry_price:
            return False, (
                f"⚠️ {symbol} is SHORT (entry {entry_price}) — a take-profit at "
                f"{new_tp_price} is at/above entry, which is a LOSS level for a short, "
                f"not a profit level. Did you mean /sl {symbol} {new_tp_price}?"
            )
        if side == "LONG" and new_tp_price <= entry_price:
            return False, (
                f"⚠️ {symbol} is LONG (entry {entry_price}) — a take-profit at "
                f"{new_tp_price} is at/below entry, which is a LOSS level for a long, "
                f"not a profit level. Did you mean /sl {symbol} {new_tp_price}?"
            )

    price_precision = pos.get("price_precision", 4)
    new_tp_price = round(new_tp_price, price_precision)

    old_order_id = pos.get("tp_order_id")
    if old_order_id and not is_simulated:
        try:
            cancel_order(symbol, old_order_id)
        except Exception as e:
            return False, f"⚠️ Failed to cancel existing TP order for {symbol}: {e}"

    close_side = "BUY" if side == "SHORT" else "SELL"  # closing a SHORT buys it back, closing a LONG sells it
    try:
        tp_resp = place_order(symbol, side=close_side, order_type="LIMIT",
                               quantity=pos["qty"], price=new_tp_price, reduce_only=True)
    except Exception as e:
        return False, f"⚠️ Failed to place new TP order for {symbol}: {e}"

    pos["tp_price"] = new_tp_price
    pos["tp_order_id"] = tp_resp["data"].get("order_id")
    save_state(open_shorts, daily_trade_tracker)
    return True, f"✅ Take-profit for {symbol} updated to {new_tp_price}."


def set_stop_loss_manual(symbol, open_shorts, daily_trade_tracker, new_sl_price):
    """Handles /sl SYMBOL PRICE — places or replaces a stop-loss trigger
    order (live) or just updates the tracked sl_price (DRY_RUN — checked
    against the live price every cycle in reconcile_open_shorts()) for one
    open position. Caller MUST already hold state_lock, same as
    close_position_manual().

    NOTE on the live order: same "best guess, unverified against CoinSwitch's
    live API docs" caveat as cancel_order() applies to order_type="STOP_MARKET"
    + trigger_price below. Test against a real (small) position before relying
    on this unattended — if the order_type name is wrong, the call will fail
    loudly (you'll get the error back in Telegram) rather than silently doing
    nothing, but confirm it actually behaves like a stop before trusting it.

    A price of 0 (or any non-positive number) removes the stop-loss —
    cancels the resting trigger order (live) and goes back to no stop-loss
    at all, same as this bot's original default. Returns (ok: bool, message:
    str) — caller sends the message to Telegram."""
    pos = open_shorts.get(symbol)
    if pos is None:
        return False, f"⚠️ No open position found for {symbol} (already closed?)."

    side = pos.get("side", "SHORT")
    entry_price = pos.get("entry_price")
    is_simulated = pos.get("simulated", DRY_RUN)

    if new_sl_price <= 0:
        old_order_id = pos.get("sl_order_id")
        if old_order_id and not is_simulated:
            try:
                cancel_order(symbol, old_order_id)
            except Exception as e:
                return False, f"⚠️ Failed to cancel existing SL order for {symbol}: {e}"
        pos["sl_price"] = None
        pos["sl_order_id"] = None
        save_state(open_shorts, daily_trade_tracker)
        return True, f"✅ Stop-loss removed for {symbol}. This position now has no stop-loss."

    # Sanity check: a stop-loss belongs on the loss side of entry — catches
    # an obvious fat-finger before it creates a "stop" that would actually
    # close the position at a profit the instant it's placed.
    if entry_price is not None:
        if side == "SHORT" and new_sl_price <= entry_price:
            return False, (
                f"⚠️ {symbol} is SHORT (entry {entry_price}) — a stop-loss at "
                f"{new_sl_price} is at/below entry, which would trigger a PROFIT close, "
                f"not protect against a loss. Did you mean /tp {symbol} {new_sl_price}?"
            )
        if side == "LONG" and new_sl_price >= entry_price:
            return False, (
                f"⚠️ {symbol} is LONG (entry {entry_price}) — a stop-loss at "
                f"{new_sl_price} is at/above entry, which would trigger a PROFIT close, "
                f"not protect against a loss. Did you mean /tp {symbol} {new_sl_price}?"
            )

    price_precision = pos.get("price_precision", 4)
    new_sl_price = round(new_sl_price, price_precision)

    old_order_id = pos.get("sl_order_id")
    if old_order_id and not is_simulated:
        try:
            cancel_order(symbol, old_order_id)
        except Exception as e:
            return False, f"⚠️ Failed to cancel existing SL order for {symbol}: {e}"

    close_side = "BUY" if side == "SHORT" else "SELL"  # closing a SHORT buys it back, closing a LONG sells it
    try:
        sl_resp = place_order(symbol, side=close_side, order_type="STOP_MARKET",
                               quantity=pos["qty"], trigger_price=new_sl_price, reduce_only=True)
    except Exception as e:
        return False, f"⚠️ Failed to place stop-loss order for {symbol}: {e}"

    pos["sl_price"] = new_sl_price
    pos["sl_order_id"] = sl_resp["data"].get("order_id")
    save_state(open_shorts, daily_trade_tracker)
    return True, f"✅ Stop-loss for {symbol} set to {new_sl_price}."


def apply_percent_tp_sl(symbol, pct, is_tp, open_shorts, daily_trade_tracker):
    """Shared by both the quick-tap TP/SL buttons (see QUICK_TPSL_PCTS /
    send_position_status_update()) and the /tppct, /slpct text commands —
    converts a flat price-move % off entry into an actual price, then
    delegates to set_take_profit_manual()/set_stop_loss_manual() to do the
    real work (cancel + replace the resting order). `pct` can be any
    positive number, not just the three quick-button presets, which is what
    lets /tppct SYMBOL 3.5 (etc.) set a custom percentage the buttons don't
    cover. Caller MUST already hold state_lock. Returns (ok: bool, message: str).

    Same convention as STRATEGY5_TP_PCT/STRATEGY5_SL_PCT and QUICK_TPSL_PCTS:
    for a USDT-margined linear perp, price-move % IS notional-value % (PnL =
    price_move_pct * notional), independent of leverage."""
    pos = open_shorts.get(symbol)
    if pos is None:
        return False, f"⚠️ No open position found for {symbol} (already closed?)."
    if pct <= 0:
        return False, f"⚠️ Percentage must be positive (got {pct})."

    entry_price = pos.get("entry_price")
    if entry_price is None:
        return False, (
            f"⚠️ {symbol} has no known entry price right now — can't compute a "
            f"{pct}% target. Try /tp or /sl with an exact price instead."
        )

    side = pos.get("side", "SHORT")
    frac = pct / 100
    if side == "SHORT":
        target_price = entry_price * (1 - frac) if is_tp else entry_price * (1 + frac)
    else:
        target_price = entry_price * (1 + frac) if is_tp else entry_price * (1 - frac)

    handler = set_take_profit_manual if is_tp else set_stop_loss_manual
    return handler(symbol, open_shorts, daily_trade_tracker, target_price)


def send_help_message():
    """Handles the /help (and /commands) command — sends a plain-text list of
    every command this bot understands, so you don't have to remember them or
    dig through the code. Doesn't touch open_shorts/daily_trade_tracker, so
    it's safe to call without state_lock, same as send_trade_history()."""
    lines = [
        "🤖 Available commands:",
        "",
        "/status — on-demand snapshot of every open position's unrealized P&L",
        "/history — closed-trade summary (win/loss, all-time P&L) + CSV file",
        "/analytics — win rate, avg win/loss, win/loss streak stats",
        "/fees — brokerage (commission), funding fees, and net P&L after fees (today + all-time)",
        f"/cooldowns — symbols currently blocked by the {ENTRY_COOLDOWN_HOURS}h re-entry rule "
        f"or the {LOSS_COOLDOWN_HOURS}h loss cooldown",
        "/debugvolume SYMBOL — raw volume-decline debug info for one symbol",
        "/tp SYMBOL PRICE — change the take-profit for an open position (0 removes it)",
        "/sl SYMBOL PRICE — set/change the stop-loss for an open position (0 removes it)",
        "/tppct SYMBOL PERCENT — same as /tp but as a % price move off entry (e.g. /tppct dogeusdt 3.5)",
        "/slpct SYMBOL PERCENT — same as /sl but as a % price move off entry",
        "/strategy — show which strategy is currently active for new trades",
        "/strategy1 — switch to Strategy 1 (resistance/RSI(77) LONG-only)",
        "/strategy2 — switch to Strategy 2 (RSI(14) 80/20 on 1h SHORT+LONG)",
        "/strategy3 — switch to Strategy 3 (wick crosses resistance but candle closes below it, then next candle closes below it, SHORT-only)",
        f"/strategy4 — switch to Strategy 4 ({STRATEGY4_SYMBOL} 15m EMA9 flip, LONG+SHORT, "
        f"{STRATEGY4_LEVERAGE}x, {STRATEGY4_TP_PRICE_MOVE_PCT:g}% TP or closes on EMA9 reversal)",
        f"/strategy5 — switch to Strategy 5 / RE Strategy ({', '.join(STRATEGY5_SYMBOLS)} "
        f"{STRATEGY5_KLINE_INTERVAL}m EMA9/EMA21 cross, LONG+SHORT, {STRATEGY5_LEVERAGE}x, "
        f"{STRATEGY5_TP_PCT:g}% TP / {STRATEGY5_SL_PCT:g}% SL, whichever hits first)",
        "/pause — stop opening new trades (existing positions still monitored)",
        "/resume — re-enable new trade entries after /pause",
        "/help or /commands — this list",
    ]
    send_telegram_message("\n".join(lines))


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


def send_cooldowns_status(daily_trade_tracker):
    """Handles the /cooldowns command — lists every symbol currently blocked
    from re-entry, whether by the plain ENTRY_COOLDOWN_HOURS re-entry rule
    (daily_trade_tracker["recent_entries"]) or by the longer
    LOSS_COOLDOWN_HOURS rule that kicks in after a losing close
    (daily_trade_tracker["recent_losses"]) — and how many hours remain until
    each is eligible again. Caller MUST already hold state_lock, same as
    /status, since both dicts are part of the shared daily_trade_tracker the
    scan loop also mutates."""
    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - ENTRY_COOLDOWN_MS
    active = {
        s: t for s, t in daily_trade_tracker.get("recent_entries", {}).items()
        if t >= cutoff_ms
    }
    loss_cutoff_ms = now_ms - LOSS_COOLDOWN_MS
    active_losses = {
        s: t for s, t in daily_trade_tracker.get("recent_losses", {}).items()
        if t >= loss_cutoff_ms
    }
    if not active and not active_losses:
        send_telegram_message(
            f"No symbols currently on the {ENTRY_COOLDOWN_HOURS}h re-entry cooldown or the "
            f"{LOSS_COOLDOWN_HOURS}h loss cooldown."
        )
        return

    lines = []
    if active:
        lines.append(f"⏳ {len(active)} symbol(s) on the {ENTRY_COOLDOWN_HOURS}h re-entry cooldown:")
        for symbol, opened_at_ms in sorted(active.items(), key=lambda kv: kv[1]):
            hours_left = (opened_at_ms + ENTRY_COOLDOWN_MS - now_ms) / (60 * 60 * 1000)
            entered_ist = datetime.datetime.fromtimestamp(opened_at_ms / 1000, IST).strftime("%Y-%m-%d %H:%M:%S")
            lines.append(f"  {symbol}: entered {entered_ist} IST — eligible again in ~{hours_left:.1f}h")
    if active_losses:
        lines.append(f"🔴 {len(active_losses)} symbol(s) on the {LOSS_COOLDOWN_HOURS}h loss cooldown:")
        for symbol, closed_at_ms in sorted(active_losses.items(), key=lambda kv: kv[1]):
            hours_left = (closed_at_ms + LOSS_COOLDOWN_MS - now_ms) / (60 * 60 * 1000)
            closed_ist = datetime.datetime.fromtimestamp(closed_at_ms / 1000, IST).strftime("%Y-%m-%d %H:%M:%S")
            lines.append(f"  {symbol}: lost on close at {closed_ist} IST — eligible again in ~{hours_left:.1f}h")
    send_telegram_message("\n".join(lines))


def send_trade_history():
    """Handles the /history command — a quick text summary of closed trades
    (win/loss count, all-time realized P&L, last 10) plus the full CSV as a
    downloadable attachment. Doesn't touch open_shorts/daily_trade_tracker,
    only the CSV file, so it's safe to call without state_lock."""
    if not os.path.exists(TRADE_HISTORY_FILE_PATH):
        send_telegram_message("No trades closed yet — history is empty.")
        return
    try:
        with open(TRADE_HISTORY_FILE_PATH, newline="") as f:
            rows = list(csv.DictReader(f))
    except Exception as e:
        send_telegram_message(f"⚠️ Couldn't read trade history: {e}")
        return

    if not rows:
        send_telegram_message("No trades closed yet — history is empty.")
        return

    total_pnl = sum(float(r["pnl_usdt"]) for r in rows if r.get("pnl_usdt"))
    wins = sum(1 for r in rows if float(r.get("pnl_usdt", 0)) >= 0)
    losses = len(rows) - wins
    recent = rows[-10:]
    lines = [
        f"{'🟢' if float(r['pnl_usdt']) >= 0 else '🔴'} {r['closed_at_ist']}  {r['symbol']}  "
        f"{float(r['pnl_usdt']):+.2f} USDT ({r['reason']}{', DRY RUN' if r.get('simulated') == 'True' else ''})"
        for r in recent
    ]
    msg = (
        f"📜 Trade history — {len(rows)} closed trade(s) total\n"
        f"Win/Loss: {wins}/{losses}  |  All-time realized P&L: {total_pnl:+.2f} USDT\n\n"
        f"Last {len(recent)}:\n" + "\n".join(lines)
    )
    send_telegram_message(msg)
    send_telegram_document(TRADE_HISTORY_FILE_PATH, caption="Full trade history (CSV)")


def compute_trade_analytics(rows):
    """Computes summary stats from closed-trade rows (as read via
    csv.DictReader from trade_history.csv). Returns a dict with:
      total_trades, wins, losses, win_rate_pct,
      gross_profit_usdt, gross_loss_usdt (positive number),
      profit_factor (gross_profit / gross_loss, None if there are no losses),
      avg_win_usdt, avg_loss_usdt (positive number),
      win_loss_ratio (avg_win / avg_loss, None if there are no losses),
      expectancy_usdt (average P&L per trade, win or lose),
      max_drawdown_usdt, max_drawdown_pct (peak-to-trough on the cumulative
      realized-P&L equity curve; pct is None if no positive peak exists yet).

    NOTE on "R:R": this bot places no fixed stop-loss (see the no-stop-loss
    warning in the CONFIG section), so there's no fixed "risk" distance to
    compute a textbook risk:reward ratio against — the downside on any given
    trade is whatever it happens to be, not a pre-defined amount. win/loss
    ratio and profit factor below are the closest meaningful substitutes
    given that.
    """
    # Rows are appended in real closing order, but sort defensively by the
    # "%Y-%m-%d %H:%M:%S" closed_at_ist string (which sorts correctly as
    # plain text) so the equity curve/drawdown calc below is never fooled by
    # e.g. a manually edited or reordered CSV.
    pnls = []
    for r in rows:
        try:
            pnls.append((r.get("closed_at_ist", ""), float(r["pnl_usdt"])))
        except (KeyError, ValueError, TypeError):
            continue
    pnls.sort(key=lambda x: x[0])

    total_trades = len(pnls)
    wins = [p for _, p in pnls if p >= 0]
    losses = [p for _, p in pnls if p < 0]

    gross_profit = sum(wins)
    gross_loss = -sum(losses)  # stored/reported as a positive number

    equity = 0.0
    peak = 0.0
    max_dd_usdt = 0.0
    max_dd_pct = None
    for _, p in pnls:
        equity += p
        if equity > peak:
            peak = equity
        drawdown = peak - equity
        if drawdown > max_dd_usdt:
            max_dd_usdt = drawdown
            max_dd_pct = (drawdown / peak * 100) if peak > 0 else None

    return {
        "total_trades": total_trades,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": (len(wins) / total_trades * 100) if total_trades else 0.0,
        "gross_profit_usdt": gross_profit,
        "gross_loss_usdt": gross_loss,
        "profit_factor": (gross_profit / gross_loss) if gross_loss > 0 else None,
        "avg_win_usdt": (gross_profit / len(wins)) if wins else 0.0,
        "avg_loss_usdt": (gross_loss / len(losses)) if losses else 0.0,
        "win_loss_ratio": ((gross_profit / len(wins)) / (gross_loss / len(losses)))
                           if wins and losses else None,
        "expectancy_usdt": (sum(p for _, p in pnls) / total_trades) if total_trades else 0.0,
        "max_drawdown_usdt": max_dd_usdt,
        "max_drawdown_pct": max_dd_pct,
    }


def send_trade_analytics():
    """Handles the /analytics command — win rate, avg win/loss, win/loss
    ratio, profit factor, expectancy, and max drawdown, all computed from the
    full trade-history CSV. Read-only against the CSV file, same as
    /history, so it's safe to call without state_lock."""
    if not os.path.exists(TRADE_HISTORY_FILE_PATH):
        send_telegram_message("No trades closed yet — nothing to analyze.")
        return
    try:
        with open(TRADE_HISTORY_FILE_PATH, newline="") as f:
            rows = list(csv.DictReader(f))
    except Exception as e:
        send_telegram_message(f"⚠️ Couldn't read trade history: {e}")
        return
    if not rows:
        send_telegram_message("No trades closed yet — nothing to analyze.")
        return

    a = compute_trade_analytics(rows)
    profit_factor_str = f"{a['profit_factor']:.2f}" if a["profit_factor"] is not None else "n/a (no losses yet)"
    win_loss_ratio_str = f"{a['win_loss_ratio']:.2f}" if a["win_loss_ratio"] is not None else "n/a"
    drawdown_pct_str = f" ({a['max_drawdown_pct']:.1f}% off peak)" if a["max_drawdown_pct"] is not None else ""

    msg = (
        f"📊 Trade analytics — {a['total_trades']} closed trade(s)\n"
        f"Win rate: {a['win_rate_pct']:.1f}%  (W {a['wins']} / L {a['losses']})\n"
        f"Avg win: {a['avg_win_usdt']:+.2f} USDT  |  Avg loss: -{a['avg_loss_usdt']:.2f} USDT\n"
        f"Win/loss ratio: {win_loss_ratio_str}  |  Profit factor: {profit_factor_str}\n"
        f"Expectancy per trade: {a['expectancy_usdt']:+.2f} USDT\n"
        f"Max drawdown (equity curve): -{a['max_drawdown_usdt']:.2f} USDT{drawdown_pct_str}\n\n"
        f"Note: no fixed stop-loss is set on these trades, so win/loss ratio "
        f"and profit factor are shown instead of a textbook risk:reward "
        f"ratio — there's no fixed risk distance to compute one against."
    )
    send_telegram_message(msg)


def send_fees_summary():
    """Handles the /fees command — total brokerage (commission) paid, plus
    funding fees and net P&L after all fees, for today (IST) and all-time.
    Read-only against CoinSwitch's Get Transactions endpoint, so it's safe
    to call without state_lock, same as /history and /analytics."""
    try:
        today = summarize_fees_and_pnl(from_time_ms=start_of_day_ist_ms(today_ist()))
        all_time = summarize_fees_and_pnl()
    except Exception as e:
        send_telegram_message(f"⚠️ Couldn't fetch fee/transaction data: {e}")
        return

    def block(label, s):
        return (
            f"{label}\n"
            f"  Gross P&L: {s['gross_pnl']:+.2f} USDT\n"
            f"  Brokerage (commission): {-s['commission']:.2f} USDT\n"
            f"  Funding fees: {s['funding_fee']:+.2f} USDT\n"
            f"  Liquidation fees: {-s['liquidation_fee']:.2f} USDT\n"
            f"  Net P&L after fees: {s['net_pnl']:+.2f} USDT"
        )

    msg = (
        f"💸 Fees & net profit{' [DRY RUN — figures will be 0 or empty]' if DRY_RUN else ''}\n\n"
        f"{block('Today (' + today_ist() + '):', today)}\n\n"
        f"{block('All-time:', all_time)}"
    )
    send_telegram_message(msg)


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

    print("  [telegram] listening for 'Close' taps and /status, /history, /analytics, /fees, "
          "/cooldowns, /debugvolume, /tp, /sl, /strategy, /strategy1, /strategy2, /strategy3, "
          "/pause, /resume, "
          "/help commands...")
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
                elif text == "/history":
                    print("  [telegram] /history requested")
                    try:
                        send_trade_history()
                    except Exception as e:
                        print(f"  [telegram] /history failed unexpectedly: {e}")
                        send_telegram_message(f"⚠️ /history failed unexpectedly: {e}")
                elif text == "/analytics":
                    print("  [telegram] /analytics requested")
                    try:
                        send_trade_analytics()
                    except Exception as e:
                        print(f"  [telegram] /analytics failed unexpectedly: {e}")
                        send_telegram_message(f"⚠️ /analytics failed unexpectedly: {e}")
                elif text == "/fees":
                    print("  [telegram] /fees requested")
                    try:
                        send_fees_summary()
                    except Exception as e:
                        print(f"  [telegram] /fees failed unexpectedly: {e}")
                        send_telegram_message(f"⚠️ /fees failed unexpectedly: {e}")
                elif text == "/cooldowns":
                    print("  [telegram] /cooldowns requested")
                    with state_lock:
                        try:
                            send_cooldowns_status(daily_trade_tracker)
                        except Exception as e:
                            print(f"  [telegram] /cooldowns failed unexpectedly: {e}")
                            send_telegram_message(f"⚠️ /cooldowns failed unexpectedly: {e}")
                elif text.startswith("/debugvolume"):
                    parts = text.split()
                    if len(parts) < 2:
                        send_telegram_message("Usage: /debugvolume SYMBOL  (e.g. /debugvolume dogeusdt)")
                    else:
                        symbol = parts[1].upper()
                        print(f"  [telegram] /debugvolume {symbol} requested")
                        try:
                            send_volume_debug(symbol)
                        except Exception as e:
                            print(f"  [telegram] /debugvolume failed unexpectedly: {e}")
                            send_telegram_message(f"⚠️ /debugvolume failed unexpectedly: {e}")
                elif text.startswith("/tppct") or text.startswith("/slpct"):
                    # Custom % version of /tp and /sl — takes a flat price-move
                    # % off entry instead of an exact price, same convention as
                    # the quick-tap TP/SL buttons (see QUICK_TPSL_PCTS) but not
                    # limited to those three presets. Checked BEFORE the plain
                    # /tp,/sl branch below since "/tppct".startswith("/tp") is
                    # also True — order matters here.
                    parts = text.split()
                    is_tp = text.startswith("/tppct")
                    cmd_name = "tppct" if is_tp else "slpct"
                    if len(parts) < 3:
                        send_telegram_message(
                            f"Usage: /{cmd_name} SYMBOL PERCENT  (e.g. /{cmd_name} dogeusdt 3.5)\n"
                            f"Sets the {'take-profit' if is_tp else 'stop-loss'} to a PERCENT price move "
                            f"off entry (same convention as the TP/SL quick-tap buttons)."
                        )
                    else:
                        symbol = parts[1].upper()
                        try:
                            pct = float(parts[2])
                        except ValueError:
                            send_telegram_message(f"⚠️ {parts[2]!r} doesn't look like a number.")
                        else:
                            print(f"  [telegram] /{cmd_name} {symbol} {pct}% requested")
                            with state_lock:
                                try:
                                    ok, msg = apply_percent_tp_sl(
                                        symbol, pct, is_tp, open_shorts, daily_trade_tracker
                                    )
                                    send_telegram_message(msg)
                                    if not ok:
                                        print(f"  [telegram] /{cmd_name} {symbol} rejected: {msg}")
                                except Exception as e:
                                    print(f"  [telegram] /{cmd_name} {symbol} failed unexpectedly: {e}")
                                    send_telegram_message(f"⚠️ /{cmd_name} {symbol} failed unexpectedly: {e}")
                elif text.startswith("/tp") or text.startswith("/sl"):
                    parts = text.split()
                    cmd_name = parts[0].lstrip("/")  # "tp" or "sl"
                    if len(parts) < 3:
                        send_telegram_message(
                            f"Usage: /{cmd_name} SYMBOL PRICE  (e.g. /{cmd_name} dogeusdt 0.1523)\n"
                            f"Price 0 removes the {'take-profit' if cmd_name == 'tp' else 'stop-loss'}."
                        )
                    else:
                        symbol = parts[1].upper()
                        try:
                            new_price = float(parts[2])
                        except ValueError:
                            send_telegram_message(f"⚠️ {parts[2]!r} doesn't look like a number.")
                        else:
                            print(f"  [telegram] /{cmd_name} {symbol} {new_price} requested")
                            with state_lock:
                                try:
                                    handler = set_take_profit_manual if cmd_name == "tp" else set_stop_loss_manual
                                    ok, msg = handler(symbol, open_shorts, daily_trade_tracker, new_price)
                                    send_telegram_message(msg)
                                    if not ok:
                                        print(f"  [telegram] /{cmd_name} {symbol} rejected: {msg}")
                                except Exception as e:
                                    print(f"  [telegram] /{cmd_name} {symbol} failed unexpectedly: {e}")
                                    send_telegram_message(f"⚠️ /{cmd_name} {symbol} failed unexpectedly: {e}")
                                    if not ok:
                                        print(f"  [telegram] /{cmd_name} {symbol} rejected: {msg}")
                                except Exception as e:
                                    print(f"  [telegram] /{cmd_name} {symbol} failed unexpectedly: {e}")
                                    send_telegram_message(f"⚠️ /{cmd_name} {symbol} failed unexpectedly: {e}")
                elif text == "/pause":
                    if bot_paused.is_set():
                        send_telegram_message("⏸ Already paused — no new trades are being opened.")
                    else:
                        bot_paused.set()
                        print("  [telegram] /pause — new entries suspended")
                        send_telegram_message(
                            "⏸ Paused. No new trades will be opened until you send /resume.\n"
                            "Existing open positions are still monitored, and 'Close' buttons still work."
                        )
                elif text == "/resume":
                    if not bot_paused.is_set():
                        send_telegram_message("▶️ Already running — not paused.")
                    else:
                        bot_paused.clear()
                        print("  [telegram] /resume — new entries re-enabled")
                        send_telegram_message("▶️ Resumed. Scanning for new entries again.")
                elif text in ("/strategy1", "/strategy2", "/strategy3", "/strategy4", "/strategy5"):
                    requested = text[-1]  # "1", "2", "3", "4", or "5"
                    with state_lock:
                        current = strategy_state.get("active", ACTIVE_STRATEGY_DEFAULT)
                        if current == requested:
                            send_telegram_message(f"Already on Strategy {requested} — no change.")
                        else:
                            strategy_state["active"] = requested
                            save_state(open_shorts, daily_trade_tracker)
                            print(f"  [telegram] switched active strategy: {current} -> {requested}")
                            name = STRATEGY_NAMES[requested]
                            send_telegram_message(
                                f"🔀 Switched to Strategy {requested} ({name}) for NEW trades.\n"
                                f"Any positions already open (from any strategy) keep being "
                                f"monitored and closed normally — this only changes what gets "
                                f"entered going forward."
                            )
                elif text == "/strategy":
                    with state_lock:
                        current = strategy_state.get("active", ACTIVE_STRATEGY_DEFAULT)
                    send_telegram_message(
                        f"Active strategy: {current}\n"
                        f"1 = resistance/RSI(77) LONG-only\n"
                        f"2 = RSI(14) 80/20 on 1h SHORT+LONG\n"
                        f"3 = resistance close-above then close-below confirmation, SHORT-only\n"
                        f"4 = {STRATEGY4_SYMBOL} 15m EMA9 flip, LONG+SHORT, {STRATEGY4_LEVERAGE}x leverage\n"
                        f"5 = RE Strategy — {', '.join(STRATEGY5_SYMBOLS)} {STRATEGY5_KLINE_INTERVAL}m EMA9/EMA21 cross, "
                        f"LONG+SHORT, {STRATEGY5_LEVERAGE}x leverage, {STRATEGY5_TP_PCT:g}% TP / "
                        f"{STRATEGY5_SL_PCT:g}% SL\n"
                        f"Switch with /strategy1, /strategy2, /strategy3, /strategy4, or /strategy5."
                    )
                elif text in ("/help", "/commands"):
                    print("  [telegram] /help requested")
                    try:
                        send_help_message()
                    except Exception as e:
                        print(f"  [telegram] /help failed unexpectedly: {e}")
                        send_telegram_message(f"⚠️ /help failed unexpectedly: {e}")
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
            if data.startswith("customhelp:"):
                try:
                    _, kind, symbol = data.split(":", 2)
                except ValueError:
                    answer_callback_query(cq.get("id", ""), "Bad button data.")
                    continue
                is_tp = kind == "tp"
                answer_callback_query(cq.get("id", ""))
                send_telegram_message(
                    f"Reply with /{'tppct' if is_tp else 'slpct'} {symbol} PERCENT to set a custom "
                    f"{'take-profit' if is_tp else 'stop-loss'} — e.g. "
                    f"/{'tppct' if is_tp else 'slpct'} {symbol} 3.5 for a 3.5% price move off entry.\n"
                    f"(Or use /{'tp' if is_tp else 'sl'} {symbol} PRICE for an exact price instead of a %.)"
                )
                continue

            if data.startswith("tppct:") or data.startswith("slpct:"):
                is_tp = data.startswith("tppct:")
                prefix_len = len("tppct:") if is_tp else len("slpct:")
                try:
                    symbol, pct_str = data[prefix_len:].rsplit(":", 1)
                    pct = float(pct_str)
                except ValueError:
                    answer_callback_query(cq.get("id", ""), "Bad button data.")
                    continue

                answer_callback_query(cq.get("id", ""), f"Setting {'TP' if is_tp else 'SL'} {pct}%...")
                with state_lock:
                    try:
                        ok, msg = apply_percent_tp_sl(symbol, pct, is_tp, open_shorts, daily_trade_tracker)
                    except Exception as e:
                        ok, msg = False, f"⚠️ Failed to set {'TP' if is_tp else 'SL'} for {symbol}: {e}"
                    send_telegram_message(msg)
                continue

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


def price_monitor_loop(open_shorts, daily_trade_tracker):
    """Background thread, independent of the 5-minute scan cycle in run_once(),
    that re-fetches live prices every PRICE_MONITOR_INTERVAL_SECONDS and
    re-runs just the loss/liquidation alert checks against them. This is what
    keeps those two alerts near-real-time instead of only being evaluated
    once per 5-minute scan — a fast adverse move can otherwise sit unflagged
    for most of a cycle.

    Deliberately does NOT touch reconcile_open_shorts(), screen_candidates(),
    or anything that opens/closes trades or does daily bookkeeping — this
    thread only ever reads prices and (maybe) sends an alert, so it can't
    race the main scan loop or the Telegram button-close handler on anything
    beyond the two flags it sets. Everything it does touch is still taken
    under state_lock, same as those other paths.

    Skips the API call entirely when there's nothing open, so an idle bot
    generates no extra request traffic."""
    print(f"  [price monitor] fast loss/liquidation check every "
          f"{PRICE_MONITOR_INTERVAL_SECONDS}s, independent of the {POLL_INTERVAL_SECONDS}s scan cycle.")
    while True:
        time.sleep(PRICE_MONITOR_INTERVAL_SECONDS)
        if not open_shorts:
            continue  # nothing open — no point spending an API call
        try:
            tickers = get_all_tickers()
        except Exception as e:
            print(f"  [price monitor] failed to fetch tickers ({e}), will retry next tick.")
            record_fetch_failure("price monitor", e)
            continue
        record_fetch_success()

        with state_lock:
            changed = False
            if check_liquidation_warnings(open_shorts, tickers):
                changed = True
            if check_loss_warnings(open_shorts, tickers):
                changed = True
            if changed:
                save_state(open_shorts, daily_trade_tracker)


def heartbeat_loop(open_shorts):
    """Background thread, independent of every other alert in this file,
    that sends a plain "still alive" ping every HEARTBEAT_INTERVAL_SECONDS.
    The daily summary and the connectivity alert both only fire under
    specific conditions — this is the one message that proves the process
    itself hasn't silently died (crashed thread, container stuck, etc.)
    even when nothing noteworthy has happened.

    Also surfaces the connectivity state's last-known-good timestamp, so a
    "the process is up but price fetches have been failing for 40 minutes"
    situation is visible here too, not just at the moment the connectivity
    alert first fired."""
    start_ms = int(time.time() * 1000)
    print(f"  [heartbeat] sending a keep-alive ping every {format_duration(HEARTBEAT_INTERVAL_SECONDS)}.")
    while True:
        time.sleep(HEARTBEAT_INTERVAL_SECONDS)
        now_ms = int(time.time() * 1000)
        uptime = format_duration((now_ms - start_ms) / 1000)

        with connectivity_lock:
            last_success_ms = connectivity_state["last_success_ms"]
            consecutive_failures = connectivity_state["consecutive_failures"]

        if last_success_ms is None:
            price_line = "no successful price fetch yet this run"
        else:
            age_seconds = (now_ms - last_success_ms) / 1000
            # Anything older than a couple of full monitor/scan cycles is
            # worth flagging even if the failure count hasn't crossed the
            # connectivity-alert threshold yet.
            stale = age_seconds > 2 * max(POLL_INTERVAL_SECONDS, PRICE_MONITOR_INTERVAL_SECONDS)
            price_line = f"last successful price fetch {format_duration(age_seconds)} ago"
            if stale:
                price_line += " ⚠️ STALE"

        with state_lock:
            open_count = len(open_shorts)

        msg = (
            f"💓 Heartbeat — bot alive, uptime {uptime}.\n"
            f"Open positions: {open_count}  |  {price_line}"
        )
        if consecutive_failures:
            msg += f"\n⚠️ {consecutive_failures} consecutive price-fetch failures in progress right now."
        if bot_paused.is_set():
            msg += "\n⏸ New entries paused (/resume to re-enable)"

        print(f"\n[heartbeat] {msg}")
        send_telegram_message(msg)


# ------------------------------ Main loop ---------------------------------------

def enter_trades_strategy1(candidates, instruments, order_margin_usdt, available_balance_usdt,
                            daily_trade_tracker, open_shorts, cooldown_cutoff_ms, now_ms,
                            loss_cooldown_cutoff_ms=None):
    """Strategy 1's entry loop — resistance/RSI(77) LONG-only signal (same
    resistance/RSI trigger logic run_once() used to run inline before strategy 2
    existed, but now opens a LONG on trigger instead of a SHORT). Only called
    when strategy 1 is the active strategy (see run_once()). Returns the
    (possibly decremented) available_balance_usdt so the caller's own wallet
    bookkeeping stays in sync across calls within the same cycle."""
    for cand in candidates:
        symbol = cand["symbol"]
        if symbol in open_shorts:
            continue
        last_entry_ms = daily_trade_tracker["recent_entries"].get(symbol)
        if last_entry_ms is not None and last_entry_ms >= cooldown_cutoff_ms:
            hours_left = (last_entry_ms + ENTRY_COOLDOWN_MS - now_ms) / (60 * 60 * 1000)
            print(f"  {symbol}: passed screening but was entered within the last "
                  f"{ENTRY_COOLDOWN_HOURS}h — skipping re-entry for another "
                  f"~{hours_left:.1f}h.")
            continue
        last_loss_ms = daily_trade_tracker.get("recent_losses", {}).get(symbol)
        if (loss_cooldown_cutoff_ms is not None and last_loss_ms is not None
                and last_loss_ms >= loss_cooldown_cutoff_ms):
            hours_left = (last_loss_ms + LOSS_COOLDOWN_MS - now_ms) / (60 * 60 * 1000)
            print(f"  {symbol}: passed screening but its last close was a LOSS within the "
                  f"last {LOSS_COOLDOWN_HOURS}h — skipping re-entry for another "
                  f"~{hours_left:.1f}h.")
            continue
        if not DRY_RUN and daily_trade_tracker["count"] >= MAX_TRADES_PER_DAY:
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
            candles = get_klines(symbol)
        except requests.HTTPError as e:
            print(f"  {symbol}: klines fetch failed ({e}), skipping.")
            continue

        resistance = evaluate_resistance(symbol, cand["last_price"], candles=candles)

        # Two INDEPENDENT ways to arrive at a SHORT signal for a symbol that
        # already passed base screening (rules 1-3: not top-200, down >5% in
        # 24h, 2cr-40cr INR volume):
        #   (a) resistance path (rule 4) — confirmed sitting at a real 15m
        #       resistance level, plus rejection candle / declining-volume
        #       checks per REQUIRE_REJECTION_CANDLE / REQUIRE_DECLINING_VOLUME.
        #   (b) RSI path (rule 4b) — 15m-candle RSI above
        #       RSI_OVERBOUGHT_SHORT_THRESHOLD, checked on its own and
        #       WITHOUT the resistance/rejection/volume-decline check. This
        #       does not stack with (a): either one on its own is enough.
        rsi_triggered, rsi_value = (False, None)
        if RSI_SHORT_ENABLED:
            rsi_triggered, rsi_value = is_rsi_overbought_short_trigger(candles)

        if resistance is None and not rsi_triggered:
            continue

        confirmations = []
        if resistance is not None:
            if REQUIRE_REJECTION_CANDLE:
                confirmations.append("rejection candle")
            if REQUIRE_DECLINING_VOLUME:
                # Note: this only means the volume check didn't block the trade —
                # it may have actually confirmed declining volume, or it may have
                # been inconclusive (unreadable field / not enough candles) and
                # been allowed through anyway. See is_volume_declining()'s True/
                # False/None return and evaluate_resistance() for the exact logic.
                confirmations.append("volume check passed")
        if rsi_triggered:
            confirmations.append(f"RSI {rsi_value:.1f} > {RSI_OVERBOUGHT_SHORT_THRESHOLD:g}")

        signal_reason = (
            f"resistance ~{resistance:.6g}" if resistance is not None else "RSI overbought (no resistance check)"
        )
        print(f"  >>> {symbol}: {cand['pct_change_24h']:.2f}% 24h, "
              f"vol {cand['quote_volume_24h_usdt']:.0f} USDT, "
              f"price {cand['last_price']}, {signal_reason} "
              f"({' + '.join(confirmations) if confirmations else 'no extra confirmations'}) — LONG signal")

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

        resp = place_order(symbol, side="BUY", order_type="MARKET", quantity=qty)
        opened_at_ms = int(time.time() * 1000)  # captured right at entry, not after the TP order below
        print(f"      order response: {resp['data']}")
        daily_trade_tracker["count"] += 1

        # Size everything downstream off what actually filled, not what we asked
        # for. Futures MARKET orders can PARTIALLY_EXECUTE with no auto-retry of
        # the remainder — and this strategy specifically targets non-top-200,
        # lower-liquidity coins, so partial fills are a real possibility, not an
        # edge case. Using the requested qty here for the reduce-only TP order
        # (or for P&L bookkeeping) would size it against a position that doesn't
        # actually exist at that size.
        try:
            filled_qty = float(resp["data"].get("exec_quantity", qty))
        except (TypeError, ValueError):
            filled_qty = qty
        if filled_qty <= 0:
            print(f"      {symbol}: order response reports 0 filled quantity — confirming against "
                  f"live positions before giving up (see confirm_fill_via_positions()). Raw: {resp['data']}")
            confirmed_qty = confirm_fill_via_positions(symbol)
            if confirmed_qty:
                filled_qty = confirmed_qty
            else:
                send_telegram_message(
                    f"⚠️ [Strategy 1] {symbol}: entry order response reported 0 filled quantity, and no "
                    f"position showed up after checking. The bot is NOT tracking this and has NOT placed "
                    f"a take-profit. If it actually filled on CoinSwitch (check the app), it is currently "
                    f"unprotected — please verify manually."
                )
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
            f"{'[DRY RUN] ' if DRY_RUN else ''}[Strategy 1] LONG {symbol}\n"
            f"Entry: {cand['last_price']} (market)\n"
            f"Qty: {qty}  |  Leverage: {leverage}x"
            f"{f' ({DESIRED_LEVERAGE}x unavailable, capped down)' if leverage < DESIRED_LEVERAGE else ''}"
            f"{f' (symbol minimum forced leverage UP from {DESIRED_LEVERAGE}x)' if leverage > DESIRED_LEVERAGE else ''}\n"
            f"24h: {cand['pct_change_24h']:.2f}%  |  Signal: {signal_reason}"
            f"{f' (RSI {rsi_value:.1f})' if rsi_triggered and resistance is not None else ''}\n"
            f"No stop-loss set on this position. Use /sl {symbol} PRICE to set one, /tp {symbol} PRICE to change the take-profit."
        )
        send_telegram_message(entry_msg)

        # Take-profit: target % return on CAPITAL, converted to a price move using
        # THIS trade's actual leverage (which may be below DESIRED_LEVERAGE).
        tp_price_pct = TP_CAPITAL_PCT / leverage
        tp_price = cand["last_price"]
        tp_order_id = None
        if TP_CAPITAL_PCT > 0:
            tp_price = round(cand["last_price"] * (1 + tp_price_pct / 100), price_precision)
            try:
                tp_resp = place_order(symbol, side="SELL", order_type="LIMIT",
                                       quantity=qty, price=tp_price, reduce_only=True)
                tp_order_id = tp_resp["data"].get("order_id")
                print(f"      take-profit @ {tp_price} "
                      f"({tp_price_pct:.2f}% price move -> {TP_CAPITAL_PCT:.1f}% on capital): {tp_resp['data']}")
                send_telegram_message(
                    f"{'[DRY RUN] ' if DRY_RUN else ''}Take-profit set for {symbol} @ {tp_price} "
                    f"({tp_price_pct:.2f}% price move -> {TP_CAPITAL_PCT:.1f}% on capital)"
                )
            except Exception as e:
                # CRITICAL: must not propagate. Before this fix, a failed TP
                # placement here raised straight out of the function and
                # skipped open_shorts[symbol]=... / save_state() below — the
                # market entry order had already filled on the exchange, so
                # the result was a real, live, completely untracked position
                # (invisible to /status, no resting TP anywhere), even though
                # the entry_msg above had already been sent to Telegram. Same
                # class of bug fixed in strategy 5's enter_trades_strategy5().
                tp_price = cand["last_price"]
                tp_order_id = None
                print(f"      {symbol}: failed to place take-profit order ({e}) — "
                      f"position will run with no take-profit until you set one manually via /tp or /tppct.")
                send_telegram_message(
                    f"⚠️ [Strategy 1] {symbol} take-profit order failed to place: {e}. "
                    f"Position is open with NO take-profit — use /tp {symbol} PRICE or "
                    f"/tppct {symbol} PERCENT to set one manually."
                )

        with state_lock:
            open_shorts[symbol] = {
                "entry_price": cand["last_price"],
                "qty": qty,
                "tp_price": tp_price,
                "tp_order_id": tp_order_id,
                "sl_price": None,               # no stop-loss by default — set manually via /sl
                "sl_order_id": None,
                "price_precision": price_precision,  # needed to round /tp and /sl prices consistently
                "opened_at_ms": opened_at_ms,
                "simulated": DRY_RUN,
                "leverage": leverage,                  # needed for the liquidation-distance estimate below
                "liquidation_warning_sent": False,      # tracks whether the 50%-to-liquidation Telegram
                                                         # alert has already fired for this position, so we
                                                         # don't re-send it every single cycle it stays past
                                                         # threshold — see check_liquidation_warnings().
                "side": "LONG",
                "strategy": "1",
            }
            # Recorded for both real and DRY_RUN entries — the no-re-entry
            # rule (ENTRY_COOLDOWN_HOURS) is a screening/behavior decision,
            # not an execution detail, so paper-trading runs should see the
            # same cooldown live trading would.
            daily_trade_tracker["recent_entries"][symbol] = opened_at_ms
            save_state(open_shorts, daily_trade_tracker)

    return available_balance_usdt


def enter_trades_strategy2(candidates, instruments, order_margin_usdt, available_balance_usdt,
                            daily_trade_tracker, open_shorts, cooldown_cutoff_ms, now_ms,
                            loss_cooldown_cutoff_ms=None):
    """Strategy 2's entry loop. `candidates` is already narrowed to non-top-200
    symbols only (see screen_candidates_v2()) — deliberately no 24h-drop-% or
    volume filter for this strategy. Signal is pure 1h-candle RSI, checked in
    both directions:
        RSI > STRATEGY2_RSI_OVERBOUGHT (80) -> SHORT
        RSI < STRATEGY2_RSI_OVERSOLD   (20) -> LONG
    Anything in between is not a signal. Leverage resolution, the daily trade
    cap, the re-entry cooldown, and wallet-balance gating all reuse the exact
    same mechanisms strategy 1 uses (resolve_leverage(), MAX_TRADES_PER_DAY,
    ENTRY_COOLDOWN_MS, available_balance_usdt) so the two strategies behave
    consistently around risk/pacing even though their signals differ."""
    for cand in candidates:
        symbol = cand["symbol"]
        if symbol in open_shorts:
            continue
        last_entry_ms = daily_trade_tracker["recent_entries"].get(symbol)
        if last_entry_ms is not None and last_entry_ms >= cooldown_cutoff_ms:
            hours_left = (last_entry_ms + ENTRY_COOLDOWN_MS - now_ms) / (60 * 60 * 1000)
            print(f"  {symbol}: passed screening but was entered within the last "
                  f"{ENTRY_COOLDOWN_HOURS}h — skipping re-entry for another "
                  f"~{hours_left:.1f}h.")
            continue
        last_loss_ms = daily_trade_tracker.get("recent_losses", {}).get(symbol)
        if (loss_cooldown_cutoff_ms is not None and last_loss_ms is not None
                and last_loss_ms >= loss_cooldown_cutoff_ms):
            hours_left = (last_loss_ms + LOSS_COOLDOWN_MS - now_ms) / (60 * 60 * 1000)
            print(f"  {symbol}: passed screening but its last close was a LOSS within the "
                  f"last {LOSS_COOLDOWN_HOURS}h — skipping re-entry for another "
                  f"~{hours_left:.1f}h.")
            continue
        if not DRY_RUN and daily_trade_tracker["count"] >= MAX_TRADES_PER_DAY:
            print("  Daily trade limit reached, no further entries until tomorrow.")
            break
        if not DRY_RUN and available_balance_usdt < order_margin_usdt:
            print(f"  [wallet] available balance {available_balance_usdt:.2f} USDT is now below "
                  f"the {order_margin_usdt:.2f} USDT needed for another trade — stopping new "
                  f"entries for this cycle.")
            break

        time.sleep(2.1)  # same KLines rate-limit pacing as strategy 1's loop

        try:
            candles = get_klines(symbol, interval=STRATEGY2_KLINE_INTERVAL, limit=STRATEGY2_LOOKBACK_CANDLES)
        except requests.HTTPError as e:
            print(f"  {symbol}: klines fetch failed ({e}), skipping.")
            continue

        rsi_value = compute_rsi(candles)
        if rsi_value is None:
            continue  # not enough closed 1h candles yet for this symbol

        if rsi_value > STRATEGY2_RSI_OVERBOUGHT:
            side = "SHORT"
        elif rsi_value < STRATEGY2_RSI_OVERSOLD:
            side = "LONG"
        else:
            continue  # RSI is in the neutral zone — no signal

        threshold = STRATEGY2_RSI_OVERBOUGHT if side == "SHORT" else STRATEGY2_RSI_OVERSOLD
        print(f"  >>> {symbol}: price {cand['last_price']}, RSI {rsi_value:.1f} "
              f"({'>' if side == 'SHORT' else '<'} {threshold:g}) — [Strategy 2] {side} signal")

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

        entry_side = "SELL" if side == "SHORT" else "BUY"
        resp = place_order(symbol, side=entry_side, order_type="MARKET", quantity=qty)
        opened_at_ms = int(time.time() * 1000)  # captured right at entry, not after the TP order below
        print(f"      order response: {resp['data']}")
        daily_trade_tracker["count"] += 1

        # Same partial-fill handling as strategy 1 — size everything
        # downstream off what actually filled, not what was requested.
        try:
            filled_qty = float(resp["data"].get("exec_quantity", qty))
        except (TypeError, ValueError):
            filled_qty = qty
        if filled_qty <= 0:
            print(f"      {symbol}: order response reports 0 filled quantity — confirming against "
                  f"live positions before giving up (see confirm_fill_via_positions()). Raw: {resp['data']}")
            confirmed_qty = confirm_fill_via_positions(symbol)
            if confirmed_qty:
                filled_qty = confirmed_qty
            else:
                send_telegram_message(
                    f"⚠️ [Strategy 2] {symbol}: entry order response reported 0 filled quantity, and no "
                    f"position showed up after checking. The bot is NOT tracking this and has NOT placed "
                    f"a take-profit. If it actually filled on CoinSwitch (check the app), it is currently "
                    f"unprotected — please verify manually."
                )
                continue
        if filled_qty != qty:
            print(f"      {symbol}: requested {qty}, filled {filled_qty} "
                  f"(partial fill) — sizing take-profit off the filled amount.")
        available_balance_usdt -= order_margin_usdt * (filled_qty / qty)
        qty = filled_qty

        entry_msg = (
            f"{'[DRY RUN] ' if DRY_RUN else ''}[Strategy 2] {side} {symbol}\n"
            f"Entry: {cand['last_price']} (market)\n"
            f"Qty: {qty}  |  Leverage: {leverage}x"
            f"{f' ({DESIRED_LEVERAGE}x unavailable, capped down)' if leverage < DESIRED_LEVERAGE else ''}"
            f"{f' (symbol minimum forced leverage UP from {DESIRED_LEVERAGE}x)' if leverage > DESIRED_LEVERAGE else ''}\n"
            f"Signal: RSI {rsi_value:.1f} on 1h chart\n"
            f"No stop-loss set on this position. Use /sl {symbol} PRICE to set one, /tp {symbol} PRICE to change the take-profit."
        )
        send_telegram_message(entry_msg)

        # Take-profit: STRATEGY2_TP_CAPITAL_PCT% return on CAPITAL EMPLOYED
        # (i.e. on the margin, NOT on the leveraged notional), converted to a
        # price move using this trade's actual leverage (which may be below
        # DESIRED_LEVERAGE) — same "% on capital -> % price move" conversion
        # strategy 1 uses, just direction-aware (short exits below entry,
        # long exits above entry).
        tp_price_pct = STRATEGY2_TP_CAPITAL_PCT / leverage
        tp_price = cand["last_price"]
        tp_order_id = None
        if STRATEGY2_TP_CAPITAL_PCT > 0:
            if side == "SHORT":
                tp_price = round(cand["last_price"] * (1 - tp_price_pct / 100), price_precision)
                tp_close_side = "BUY"
            else:
                tp_price = round(cand["last_price"] * (1 + tp_price_pct / 100), price_precision)
                tp_close_side = "SELL"
            try:
                tp_resp = place_order(symbol, side=tp_close_side, order_type="LIMIT",
                                       quantity=qty, price=tp_price, reduce_only=True)
                tp_order_id = tp_resp["data"].get("order_id")
                print(f"      take-profit @ {tp_price} "
                      f"({tp_price_pct:.2f}% price move -> {STRATEGY2_TP_CAPITAL_PCT:.1f}% on capital): {tp_resp['data']}")
                send_telegram_message(
                    f"{'[DRY RUN] ' if DRY_RUN else ''}Take-profit set for {symbol} @ {tp_price} "
                    f"({tp_price_pct:.2f}% price move -> {STRATEGY2_TP_CAPITAL_PCT:.1f}% on capital)"
                )
            except Exception as e:
                # CRITICAL: must not propagate — same class of bug fixed in
                # strategy 5's enter_trades_strategy5(). A failed TP here
                # used to raise straight out of the function and skip
                # open_shorts[symbol]=.../save_state() below, leaving a
                # real, live, completely untracked position even though the
                # entry_msg had already gone to Telegram.
                tp_price = cand["last_price"]
                tp_order_id = None
                print(f"      {symbol}: failed to place take-profit order ({e}) — "
                      f"position will run with no take-profit until you set one manually via /tp or /tppct.")
                send_telegram_message(
                    f"⚠️ [Strategy 2] {symbol} take-profit order failed to place: {e}. "
                    f"Position is open with NO take-profit — use /tp {symbol} PRICE or "
                    f"/tppct {symbol} PERCENT to set one manually."
                )

        with state_lock:
            open_shorts[symbol] = {
                "entry_price": cand["last_price"],
                "qty": qty,
                "tp_price": tp_price,
                "tp_order_id": tp_order_id,
                "sl_price": None,               # no stop-loss by default — set manually via /sl
                "sl_order_id": None,
                "price_precision": price_precision,
                "opened_at_ms": opened_at_ms,
                "simulated": DRY_RUN,
                "leverage": leverage,
                "liquidation_warning_sent": False,
                "side": side,
                "strategy": "2",
            }
            daily_trade_tracker["recent_entries"][symbol] = opened_at_ms
            save_state(open_shorts, daily_trade_tracker)

    return available_balance_usdt


def enter_trades_strategy3(candidates, instruments, order_margin_usdt, available_balance_usdt,
                            daily_trade_tracker, open_shorts, cooldown_cutoff_ms, now_ms,
                            loss_cooldown_cutoff_ms=None):
    """Strategy 3's entry loop. `candidates` uses the exact same screening as
    strategy 1 (screen_candidates(): not top-200, down >5% in 24h, 2cr-40cr INR
    volume). Signal is resistance-only (no RSI path) on the 15m chart, same
    level detection as strategy 1 (find_resistance_levels()), but:
        - does NOT require has_rejection_candle()'s strict wick>=body test —
          instead requires a break-and-reject: closed candle N closes ABOVE
          a resistance level (arms a pending confirmation, no entry yet),
          and only if the very next closed candle N+1 closes BELOW that same
          level does this function enter (see
          get_strategy3_confirmed_resistance()). This is entirely off
          closed-candle data, not the live ticker price.
        - DOES actually place a SHORT (SELL), unlike strategy 1's LONG bug
    Volume-decline filtering still applies per STRATEGY3_REQUIRE_DECLINING_VOLUME.
    Leverage resolution, the daily trade cap, the re-entry cooldown, and
    wallet-balance gating all reuse the exact same mechanisms strategy 1 and
    2 use, for consistent risk/pacing across all three strategies."""
    for cand in candidates:
        symbol = cand["symbol"]
        if symbol in open_shorts:
            continue
        last_entry_ms = daily_trade_tracker["recent_entries"].get(symbol)
        if last_entry_ms is not None and last_entry_ms >= cooldown_cutoff_ms:
            hours_left = (last_entry_ms + ENTRY_COOLDOWN_MS - now_ms) / (60 * 60 * 1000)
            print(f"  {symbol}: passed screening but was entered within the last "
                  f"{ENTRY_COOLDOWN_HOURS}h — skipping re-entry for another "
                  f"~{hours_left:.1f}h.")
            continue
        last_loss_ms = daily_trade_tracker.get("recent_losses", {}).get(symbol)
        if (loss_cooldown_cutoff_ms is not None and last_loss_ms is not None
                and last_loss_ms >= loss_cooldown_cutoff_ms):
            hours_left = (last_loss_ms + LOSS_COOLDOWN_MS - now_ms) / (60 * 60 * 1000)
            print(f"  {symbol}: passed screening but its last close was a LOSS within the "
                  f"last {LOSS_COOLDOWN_HOURS}h — skipping re-entry for another "
                  f"~{hours_left:.1f}h.")
            continue
        if not DRY_RUN and daily_trade_tracker["count"] >= MAX_TRADES_PER_DAY:
            print("  Daily trade limit reached, no further entries until tomorrow.")
            break
        if not DRY_RUN and available_balance_usdt < order_margin_usdt:
            print(f"  [wallet] available balance {available_balance_usdt:.2f} USDT is now below "
                  f"the {order_margin_usdt:.2f} USDT needed for another trade — stopping new "
                  f"entries for this cycle.")
            break

        time.sleep(2.1)  # same KLines rate-limit pacing as strategies 1 and 2

        try:
            candles = get_klines(symbol)  # 15m chart, same interval as strategy 1
        except requests.HTTPError as e:
            print(f"  {symbol}: klines fetch failed ({e}), skipping.")
            continue

        resistance = get_strategy3_confirmed_resistance(symbol, candles)
        if resistance is None:
            continue
        if STRATEGY3_REQUIRE_DECLINING_VOLUME and is_volume_declining(candles) is False:
            # Only a confirmed False (volume readable and NOT declining) blocks
            # the trade — None (unreadable/insufficient data) falls through,
            # same "don't let a missing field silently kill every signal"
            # behavior evaluate_resistance() used.
            continue

        print(f"  >>> {symbol}: {cand['pct_change_24h']:.2f}% 24h, "
              f"vol {cand['quote_volume_24h_usdt']:.0f} USDT, "
              f"price {cand['last_price']}, resistance ~{resistance:.6g} "
              f"(close-above then close-below confirmation) — [Strategy 3] SHORT signal")

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

        # Same partial-fill handling as strategies 1 and 2 — size everything
        # downstream off what actually filled, not what was requested.
        try:
            filled_qty = float(resp["data"].get("exec_quantity", qty))
        except (TypeError, ValueError):
            filled_qty = qty
        if filled_qty <= 0:
            print(f"      {symbol}: order response reports 0 filled quantity — confirming against "
                  f"live positions before giving up (see confirm_fill_via_positions()). Raw: {resp['data']}")
            confirmed_qty = confirm_fill_via_positions(symbol)
            if confirmed_qty:
                filled_qty = confirmed_qty
            else:
                send_telegram_message(
                    f"⚠️ [Strategy 3] {symbol}: entry order response reported 0 filled quantity, and no "
                    f"position showed up after checking. The bot is NOT tracking this and has NOT placed "
                    f"a take-profit. If it actually filled on CoinSwitch (check the app), it is currently "
                    f"unprotected — please verify manually."
                )
                continue
        if filled_qty != qty:
            print(f"      {symbol}: requested {qty}, filled {filled_qty} "
                  f"(partial fill) — sizing take-profit off the filled amount.")
        available_balance_usdt -= order_margin_usdt * (filled_qty / qty)
        qty = filled_qty

        entry_msg = (
            f"{'[DRY RUN] ' if DRY_RUN else ''}[Strategy 3] SHORT {symbol}\n"
            f"Entry: {cand['last_price']} (market)\n"
            f"Qty: {qty}  |  Leverage: {leverage}x"
            f"{f' ({DESIRED_LEVERAGE}x unavailable, capped down)' if leverage < DESIRED_LEVERAGE else ''}"
            f"{f' (symbol minimum forced leverage UP from {DESIRED_LEVERAGE}x)' if leverage > DESIRED_LEVERAGE else ''}\n"
            f"24h: {cand['pct_change_24h']:.2f}%  |  Signal: resistance ~{resistance:.6g} "
            f"(closed above level, then next candle closed below it)\n"
            f"No stop-loss set on this position. Use /sl {symbol} PRICE to set one, /tp {symbol} PRICE to change the take-profit."
        )
        send_telegram_message(entry_msg)

        # Take-profit: STRATEGY3_TP_CAPITAL_PCT% return on CAPITAL, same
        # "% on capital -> % price move" conversion as strategies 1 and 2.
        # Short exits below entry.
        tp_price_pct = STRATEGY3_TP_CAPITAL_PCT / leverage
        tp_price = cand["last_price"]
        tp_order_id = None
        if STRATEGY3_TP_CAPITAL_PCT > 0:
            tp_price = round(cand["last_price"] * (1 - tp_price_pct / 100), price_precision)
            try:
                tp_resp = place_order(symbol, side="BUY", order_type="LIMIT",
                                       quantity=qty, price=tp_price, reduce_only=True)
                tp_order_id = tp_resp["data"].get("order_id")
                print(f"      take-profit @ {tp_price} "
                      f"({tp_price_pct:.2f}% price move -> {STRATEGY3_TP_CAPITAL_PCT:.1f}% on capital): {tp_resp['data']}")
                send_telegram_message(
                    f"{'[DRY RUN] ' if DRY_RUN else ''}Take-profit set for {symbol} @ {tp_price} "
                    f"({tp_price_pct:.2f}% price move -> {STRATEGY3_TP_CAPITAL_PCT:.1f}% on capital)"
                )
            except Exception as e:
                # CRITICAL: must not propagate — same class of bug fixed in
                # strategy 5's enter_trades_strategy5().
                tp_price = cand["last_price"]
                tp_order_id = None
                print(f"      {symbol}: failed to place take-profit order ({e}) — "
                      f"position will run with no take-profit until you set one manually via /tp or /tppct.")
                send_telegram_message(
                    f"⚠️ [Strategy 3] {symbol} take-profit order failed to place: {e}. "
                    f"Position is open with NO take-profit — use /tp {symbol} PRICE or "
                    f"/tppct {symbol} PERCENT to set one manually."
                )

        with state_lock:
            open_shorts[symbol] = {
                "entry_price": cand["last_price"],
                "qty": qty,
                "tp_price": tp_price,
                "tp_order_id": tp_order_id,
                "sl_price": None,               # no stop-loss by default — set manually via /sl
                "sl_order_id": None,
                "price_precision": price_precision,
                "opened_at_ms": opened_at_ms,
                "simulated": DRY_RUN,
                "leverage": leverage,
                "liquidation_warning_sent": False,
                "side": "SHORT",
                "strategy": "3",
            }
            daily_trade_tracker["recent_entries"][symbol] = opened_at_ms
            save_state(open_shorts, daily_trade_tracker)

    return available_balance_usdt


def enter_trades_strategy4(instruments, usdt_inr_rate, available_balance_usdt,
                            daily_trade_tracker, open_shorts, now_ms):
    """Strategy 4's entry loop — BTC-only 15m EMA9 flip system. Only called
    when strategy 4 is the active strategy (see run_once()).

    Unlike strategies 1-3, this does NOT go through screen_candidates()/
    screen_candidates_v2() at all — it always looks at exactly one fixed
    symbol (STRATEGY4_SYMBOL, default BTCUSDT), since the whole point is a
    dedicated always-on BTC scalper, not a market-wide screener.

    Rule (both directions off the same 15m EMA9 line):
        - flat + latest CLOSED 15m candle closes ABOVE EMA9 -> go LONG
        - flat + latest CLOSED 15m candle closes BELOW EMA9 -> go SHORT
    Take-profit is a flat STRATEGY4_TP_PRICE_MOVE_PCT (0.3%) price move (not
    a %-of-capital figure like strategies 1-3 use), placed as a resting
    reduce-only limit order same as the others. The OTHER way this position
    can close — a later candle closing back across EMA9 against it — is
    handled every cycle regardless of the active strategy by
    check_strategy4_signal_exits(), not here.

    Deliberately skips the shared ENTRY_COOLDOWN_HOURS / LOSS_COOLDOWN_HOURS
    re-entry cooldowns and the shared MAX_TRADES_PER_DAY cap that strategies
    1-3 use — per instruction, this strategy is meant to flip position as
    often as the 15m EMA9 signal changes and take as many trades as it can
    in a day. The only real constraints left are: never open a second
    position while one is already open on STRATEGY4_SYMBOL (the check right
    below), and (in live trading) having enough free wallet balance for the
    next trade's margin. Returns the (possibly decremented)
    available_balance_usdt so the caller's wallet bookkeeping stays in sync,
    same contract as enter_trades_strategy1/2/3."""
    symbol = STRATEGY4_SYMBOL
    if symbol in open_shorts:
        # Already long or short — check_strategy4_signal_exits() (an EMA9
        # reversal) and the resting take-profit order are what close it;
        # nothing to do here until it's flat again.
        return available_balance_usdt

    order_margin_usdt = STRATEGY4_CAPITAL_INR / usdt_inr_rate
    if not DRY_RUN and available_balance_usdt < order_margin_usdt:
        print(f"  [strategy4] available balance {available_balance_usdt:.2f} USDT is below the "
              f"{order_margin_usdt:.2f} USDT ({STRATEGY4_CAPITAL_INR:,} INR) needed for the next "
              f"{symbol} trade — skipping this cycle.")
        return available_balance_usdt

    try:
        candles = get_klines(symbol, interval=STRATEGY4_KLINE_INTERVAL, limit=STRATEGY4_LOOKBACK_CANDLES)
    except Exception as e:
        print(f"  [strategy4] {symbol}: klines fetch failed ({e}), skipping this cycle.")
        return available_balance_usdt
    if not candles:
        print(f"  [strategy4] {symbol}: no candles returned, skipping this cycle.")
        return available_balance_usdt

    ema_series = compute_ema_series(candles, STRATEGY4_EMA_PERIOD)
    if ema_series[-1] is None:
        print(f"  [strategy4] {symbol}: not enough candles yet for EMA{STRATEGY4_EMA_PERIOD}, skipping.")
        return available_balance_usdt

    latest_close = float(candles[-1]["c"])
    latest_ema = ema_series[-1]

    if latest_close > latest_ema:
        side = "LONG"
    elif latest_close < latest_ema:
        side = "SHORT"
    else:
        print(f"  [strategy4] {symbol}: latest close exactly equals EMA{STRATEGY4_EMA_PERIOD}, "
              f"no signal this cycle.")
        return available_balance_usdt

    print(f"  >>> [strategy4] {symbol}: latest 15m candle closed {latest_close:.6g} vs EMA9 "
          f"{latest_ema:.6g} -> {side} signal")

    instrument = instruments.get(symbol)
    if instrument is None:
        print(f"      [strategy4] no instrument info for {symbol}, skipping order.")
        return available_balance_usdt

    leverage = resolve_leverage(instrument, desired=STRATEGY4_LEVERAGE)
    if leverage < STRATEGY4_LEVERAGE:
        print(f"      [strategy4] {symbol}: {STRATEGY4_LEVERAGE}x not available, using max "
              f"{leverage}x instead.")
    elif leverage > STRATEGY4_LEVERAGE:
        print(f"      [strategy4] {symbol}: this symbol's own minimum leverage ({leverage}x) is "
              f"above {STRATEGY4_LEVERAGE}x — trading at {leverage}x instead, which is MORE "
              f"leverage than desired.")
    set_leverage(symbol, leverage)

    entry_price = latest_close  # market order will fill close to the last closed candle's close
    qty = compute_quantity(entry_price, order_margin_usdt, leverage, instrument)
    price_precision = int(instrument.get("price_precision", 4))

    entry_side = "BUY" if side == "LONG" else "SELL"
    resp = place_order(symbol, side=entry_side, order_type="MARKET", quantity=qty)
    opened_at_ms = int(time.time() * 1000)  # captured right at entry, not after the TP order below
    print(f"      [strategy4] order response: {resp['data']}")
    daily_trade_tracker["count"] += 1

    # Same partial-fill handling as strategies 1-3 — size everything
    # downstream off what actually filled, not what was requested.
    try:
        filled_qty = float(resp["data"].get("exec_quantity", qty))
    except (TypeError, ValueError):
        filled_qty = qty
    if filled_qty <= 0:
        print(f"      [strategy4] {symbol}: order response reports 0 filled quantity — confirming "
              f"against live positions before giving up (see confirm_fill_via_positions()). Raw: {resp['data']}")
        confirmed_qty = confirm_fill_via_positions(symbol)
        if confirmed_qty:
            filled_qty = confirmed_qty
        else:
            send_telegram_message(
                f"⚠️ [Strategy 4] {symbol}: entry order response reported 0 filled quantity, and no "
                f"position showed up after checking. The bot is NOT tracking this and has NOT placed "
                f"a take-profit. If it actually filled on CoinSwitch (check the app), it is currently "
                f"unprotected — please verify manually."
            )
            return available_balance_usdt
    if filled_qty != qty:
        print(f"      [strategy4] {symbol}: requested {qty}, filled {filled_qty} "
              f"(partial fill) — sizing take-profit off the filled amount.")
    available_balance_usdt -= order_margin_usdt * (filled_qty / qty)
    qty = filled_qty

    # Take-profit: a flat STRATEGY4_TP_PRICE_MOVE_PCT (0.3%) price move in
    # this trade's favor — direction-aware, same shape as strategy 2's TP
    # but expressed directly as a price move rather than %-on-capital.
    tp_pct = STRATEGY4_TP_PRICE_MOVE_PCT / 100
    if side == "LONG":
        tp_price = round(entry_price * (1 + tp_pct), price_precision)
        tp_close_side = "SELL"
    else:
        tp_price = round(entry_price * (1 - tp_pct), price_precision)
        tp_close_side = "BUY"

    tp_resp = None
    tp_order_id = None
    try:
        tp_resp = place_order(symbol, side=tp_close_side, order_type="LIMIT",
                               quantity=qty, price=tp_price, reduce_only=True)
        tp_order_id = tp_resp["data"].get("order_id")
        print(f"      [strategy4] take-profit @ {tp_price} ({STRATEGY4_TP_PRICE_MOVE_PCT:g}% price "
              f"move): {tp_resp['data']}")
    except Exception as e:
        # CRITICAL: must not propagate — same class of bug fixed in
        # strategy 5's enter_trades_strategy5(). A failed TP here used to
        # raise straight out of the function and skip open_shorts[symbol]=
        # .../save_state() AND the entry_msg below, leaving a real, live,
        # completely untracked position.
        tp_price = None
        tp_order_id = None
        print(f"      [strategy4] {symbol}: failed to place take-profit order ({e}) — "
              f"position will run with no take-profit until you set one manually via /tp or /tppct.")
        send_telegram_message(
            f"⚠️ [Strategy 4] {symbol} take-profit order failed to place: {e}. "
            f"Position is open with NO take-profit — use /tp {symbol} PRICE or "
            f"/tppct {symbol} PERCENT to set one manually."
        )

    entry_msg = (
        f"{'[DRY RUN] ' if DRY_RUN else ''}[Strategy 4] {side} {symbol}\n"
        f"Entry: {entry_price} (market)  |  Qty: {qty}  |  Leverage: {leverage}x"
        f"{f' ({STRATEGY4_LEVERAGE}x unavailable, capped down)' if leverage < STRATEGY4_LEVERAGE else ''}"
        f"{f' (symbol minimum forced leverage UP from {STRATEGY4_LEVERAGE}x)' if leverage > STRATEGY4_LEVERAGE else ''}\n"
        f"Signal: 15m candle closed {'above' if side == 'LONG' else 'below'} EMA9 "
        f"({latest_close:.6g} vs {latest_ema:.6g})\n"
        + (f"Take-profit @ {tp_price} ({STRATEGY4_TP_PRICE_MOVE_PCT:g}% price move) — OR closes early "
           f"if a later 15m candle closes back across EMA9 against this position.\n"
           if tp_price is not None else
           "No take-profit set (order failed) — closes on a later 15m candle closing back across EMA9.\n")
        + "No stop-loss set. Use /sl " + symbol + " PRICE to set one."
    )
    send_telegram_message(entry_msg)

    with state_lock:
        open_shorts[symbol] = {
            "entry_price": entry_price,
            "qty": qty,
            "tp_price": tp_price,
            "tp_order_id": tp_order_id,
            "sl_price": None,               # no stop-loss by default — set manually via /sl
            "sl_order_id": None,
            "price_precision": price_precision,
            "opened_at_ms": opened_at_ms,
            "simulated": DRY_RUN,
            "leverage": leverage,
            "liquidation_warning_sent": False,
            "side": side,
            "strategy": "4",
        }
        # Recorded for visibility in /cooldowns etc., even though strategy 4's
        # own entry logic above never checks it — see the docstring for why
        # this strategy is exempt from the shared re-entry cooldown.
        daily_trade_tracker["recent_entries"][symbol] = opened_at_ms
        save_state(open_shorts, daily_trade_tracker)

    return available_balance_usdt


def enter_trades_strategy5(instruments, usdt_inr_rate, available_balance_usdt,
                            daily_trade_tracker, open_shorts, now_ms):
    """Strategy 5's ("RE Strategy") entry loop — a fixed-symbol-list EMA9/EMA21
    crossover system, ported live from backtest_strategy_ema9_ema21_cross.py.
    Only called when strategy 5 is the active strategy (see run_once()).
    Like strategy 4, this does NOT go through screen_candidates() at all — it
    only ever looks at the fixed symbols in STRATEGY5_SYMBOLS (default
    REUSDT, CCUSDT, DEEPUSDT, CRVUSDT, ARBUSDT, TREEUSDT, PLUMEUSDT,
    AEROUSDT, ARXUSDT, EIGENUSDT), evaluated independently one at a time in the
    loop below.

    Rule, evaluated on the latest CLOSED candle only (see
    compute_ema_cross_signal()): flat + a fresh EMA9/EMA21 crossover event on
    that candle, confirmed by the same candle's own close -> enter in that
    direction.

    UNLIKE strategy 4, there is NO signal-reversal exit for this strategy —
    a position closes ONLY on whichever of its take-profit or stop-loss
    resting orders fills first (STRATEGY5_TP_PCT / STRATEGY5_SL_PCT, flat
    price-move %s off entry), matching the backtest's "TP or SL, whichever
    hits first" rule exactly. Both orders are placed right here at entry
    time. reconcile_open_shorts() already knows how to check both a tp_price
    and sl_price for DRY_RUN positions (SL checked before TP each cycle), and
    for real trades just detects the position's closure on the exchange
    regardless of which resting order filled it.

    Same as strategy 4: exempt from the shared ENTRY_COOLDOWN_HOURS/
    LOSS_COOLDOWN_HOURS re-entry cooldowns and the MAX_TRADES_PER_DAY cap.
    It DOES have its own re-entry cooldown though: STRATEGY5_REENTRY_COOLDOWN_HOURS
    (default 1h) blocks re-entering a symbol for that many hours from the
    moment its last position closed, win or loss alike — see
    daily_trade_tracker["recent_closes"] / record_recent_close(). Otherwise
    the only gates per-symbol are already having an open position on that
    symbol, or (in live trading) not having enough free wallet balance left
    for the next trade's margin — checked twice: once cheaply against the
    cycle-start snapshot before spending a klines fetch, then again against
    a freshly re-fetched live balance immediately before the order is
    placed, so a balance that changed (or was already stale) mid-cycle
    can't slip through and hit an exchange-side "Insufficient balance"
    rejection. Returns the (possibly decremented) available_balance_usdt,
    same contract as enter_trades_strategy1/2/3/4."""
    for symbol in STRATEGY5_SYMBOLS:
        if symbol in open_shorts:
            # Already in a position on this symbol — it closes via its
            # resting TP/SL orders, picked up next cycle by
            # reconcile_open_shorts(). Nothing to do here until it's flat
            # again. Move on and check the next symbol in the list.
            continue

        last_close_ms = daily_trade_tracker.get("recent_closes", {}).get(symbol)
        if last_close_ms is not None and now_ms - last_close_ms < STRATEGY5_REENTRY_COOLDOWN_MS:
            remaining_min = (STRATEGY5_REENTRY_COOLDOWN_MS - (now_ms - last_close_ms)) / 60000
            print(f"  [strategy5] {symbol}: closed {((now_ms - last_close_ms) / 60000):.1f} min ago — "
                  f"still within the {STRATEGY5_REENTRY_COOLDOWN_HOURS:g}h re-entry cooldown "
                  f"({remaining_min:.1f} min remaining), skipping this cycle.")
            continue

        # Coarse check using the cycle-start balance snapshot, just to avoid
        # wasting a klines fetch when it's obviously not enough. The
        # authoritative check happens right before order placement below,
        # against a freshly re-fetched balance. Requires the fee buffer on
        # top of the trade minimum too, so this can't pass a balance that's
        # only enough for the margin with nothing left for fees.
        if not DRY_RUN and available_balance_usdt < STRATEGY5_MIN_TRADE_USDT + STRATEGY5_FEE_BUFFER_USDT:
            print(f"  [strategy5] available balance {available_balance_usdt:.2f} USDT is below the "
                  f"{STRATEGY5_MIN_TRADE_USDT + STRATEGY5_FEE_BUFFER_USDT:.2f} USDT minimum needed "
                  f"({STRATEGY5_MIN_TRADE_USDT:.0f} USDT trade + {STRATEGY5_FEE_BUFFER_USDT:.2f} USDT "
                  f"fee buffer) for the next {symbol} trade — skipping this symbol this cycle.")
            continue

        try:
            candles = get_klines(symbol, interval=STRATEGY5_KLINE_INTERVAL, limit=STRATEGY5_LOOKBACK_CANDLES)
        except Exception as e:
            print(f"  [strategy5] {symbol}: klines fetch failed ({e}), skipping this cycle.")
            continue
        if not candles:
            print(f"  [strategy5] {symbol}: no candles returned, skipping this cycle.")
            continue

        signal_side = compute_ema_cross_signal(candles, STRATEGY5_EMA_FAST, STRATEGY5_EMA_SLOW)
        side = signal_side[-1]
        if side is None:
            continue

        latest_close = float(candles[-1]["c"])
        print(f"  >>> [strategy5] {symbol}: fresh EMA{STRATEGY5_EMA_FAST}/EMA{STRATEGY5_EMA_SLOW} "
              f"crossover confirmed by close ({latest_close:.6g}) on the latest closed "
              f"{STRATEGY5_KLINE_INTERVAL}m candle -> {side} signal")

        instrument = instruments.get(symbol)
        if instrument is None:
            print(f"      [strategy5] no instrument info for {symbol}, skipping order.")
            continue

        leverage = resolve_leverage(instrument, desired=STRATEGY5_LEVERAGE)
        if leverage < STRATEGY5_LEVERAGE:
            print(f"      [strategy5] {symbol}: {STRATEGY5_LEVERAGE}x not available, using max "
                  f"{leverage}x instead.")
        elif leverage > STRATEGY5_LEVERAGE:
            print(f"      [strategy5] {symbol}: this symbol's own minimum leverage ({leverage}x) is "
                  f"above {STRATEGY5_LEVERAGE}x — trading at {leverage}x instead, which is MORE "
                  f"leverage than desired.")
        try:
            set_leverage(symbol, leverage)
        except Exception as e:
            # If the exchange rejects/ignores this, the account may still be
            # sitting at whatever leverage it had before — which can silently
            # break the margin math below (notional / intended leverage) if
            # the real effective leverage ends up lower. Log it loudly and
            # keep going rather than abort the whole cycle; the sizing log
            # line right below will show whether that's what happened.
            print(f"      [strategy5] {symbol}: set_leverage({leverage}x) failed ({e}) — "
                  f"the exchange may still be using a different leverage than intended, which can "
                  f"cause an 'Insufficient balance' rejection even with a healthy free balance.")

        # Re-fetch the wallet balance right here, immediately before placing
        # the order — the cycle-start snapshot (available_balance_usdt) can
        # be stale by the time we get this far (klines fetch, EMA calc,
        # leverage resolution, and any earlier symbols in this same loop
        # that already spent margin all take time/money). This is exactly
        # what caused a live "Insufficient balance" rejection despite the
        # cycle-start snapshot showing enough. Falls back to the
        # cycle-start value if the re-fetch itself fails, rather than
        # aborting the whole entry over a transient balance-check error.
        if not DRY_RUN:
            try:
                available_balance_usdt = get_wallet_balance(usdt_inr_rate)["total_usdt"]
            except Exception as e:
                print(f"      [strategy5] {symbol}: live balance re-check failed ({e}), falling back "
                      f"to the cycle-start snapshot ({available_balance_usdt:.2f} USDT).")
            if available_balance_usdt < STRATEGY5_MIN_TRADE_USDT + STRATEGY5_FEE_BUFFER_USDT:
                print(f"      [strategy5] {symbol}: live balance {available_balance_usdt:.2f} USDT is "
                      f"below the {STRATEGY5_MIN_TRADE_USDT + STRATEGY5_FEE_BUFFER_USDT:.2f} USDT "
                      f"minimum (trade + fee buffer) — skipping this trade "
                      f"(cycle-start snapshot said it was enough).")
                continue

        order_margin_usdt = (
            min(available_balance_usdt - STRATEGY5_FEE_BUFFER_USDT, STRATEGY5_MAX_TRADE_USDT) if not DRY_RUN
            else STRATEGY5_MAX_TRADE_USDT
        )

        # Always log the exact sizing inputs right before the order goes
        # out — margin, leverage, and resulting notional — so an
        # "Insufficient balance" rejection despite a healthy free balance
        # (as seen live on SAHARAUSDT) can be diagnosed from the logs
        # instead of guessed at. In particular this makes it possible to
        # tell whether the exchange is actually honoring
        # STRATEGY5_LEVERAGE — if it silently caps leverage lower than what
        # instrument.get("max_leverage") reports, the real required margin
        # would be notional / actual_leverage, which can be far above what
        # this bot thinks it's sending.
        print(f"      [strategy5] {symbol}: sizing entry — margin {order_margin_usdt:.2f} USDT, "
              f"leverage {leverage}x (instrument max_leverage reported: "
              f"{instrument.get('max_leverage', 'unknown')}), notional "
              f"{order_margin_usdt * leverage:.2f} USDT, free balance {available_balance_usdt:.2f} USDT")

        price_precision = int(instrument.get("price_precision", 4))
        entry_price = round(latest_close, price_precision)  # for sizing/TP/SL calc; MARKET order will fill close to this
        qty = compute_quantity(entry_price, order_margin_usdt, leverage, instrument)

        entry_side = "BUY" if side == "LONG" else "SELL"
        try:
            resp = place_order(symbol, side=entry_side, order_type="MARKET", quantity=qty)
            opened_at_ms = int(time.time() * 1000)  # captured right at entry, not after the TP/SL orders below
            print(f"      [strategy5] order response: {resp['data']}")
        except Exception as e:
            # This is the last unguarded gap in this function — and the most
            # dangerous one, because a MARKET order can genuinely execute on
            # CoinSwitch's side even when the HTTP response never makes it
            # back here (timeout, dropped connection, etc), or come back 200
            # with an unexpected shape (missing 'data', caught here too since
            # resp['data'] is now inside this same try). Before this fix,
            # either case propagated straight out of enter_trades_strategy5()
            # and skipped EVERYTHING: no TP/SL, no open_shorts tracking, no
            # Telegram message of any kind — a trade that shows up in the
            # CoinSwitch app but leaves absolutely no trace here. We
            # genuinely can't tell from this exception alone whether the
            # order filled, so rather than guess (and either fabricate a
            # phantom tracked position or silently drop a real one), alert
            # loudly and tell you to check manually — this is the one case
            # where "don't abort" isn't safe, because we don't have a fill
            # confirmation to safely build a tracked position from.
            print(f"  [strategy5] {symbol}: entry order request failed/errored ({e}) — "
                  f"UNKNOWN whether it actually filled on the exchange.")
            send_telegram_message(
                f"⚠️ [Strategy 5] {symbol} entry order request failed/errored: {e}\n"
                f"This may or may not have actually filled on CoinSwitch — please check the app "
                f"manually. If it filled, it is NOT yet tracked by the bot (no TP/SL set); it will "
                f"be picked up automatically on the next restart, or set TP/SL now with /tp, /sl, "
                f"/tppct, or /slpct once you confirm it in the app."
            )
            continue
        daily_trade_tracker["count"] += 1



        # Same partial-fill handling as strategies 1-4 — size everything
        # downstream off what actually filled, not what was requested.
        try:
            filled_qty = float(resp["data"].get("exec_quantity", qty))
        except (TypeError, ValueError):
            filled_qty = qty
        if filled_qty <= 0:
            print(f"      [strategy5] {symbol}: order response reports 0 filled quantity — confirming "
                  f"against live positions before giving up (see confirm_fill_via_positions()). Raw: {resp['data']}")
            confirmed_qty = confirm_fill_via_positions(symbol)
            if confirmed_qty:
                filled_qty = confirmed_qty
            else:
                send_telegram_message(
                    f"⚠️ [Strategy 5] {symbol}: entry order response reported 0 filled quantity, and no "
                    f"position showed up after checking. The bot is NOT tracking this and has NOT placed "
                    f"TP/SL. If it actually filled on CoinSwitch (check the app), it is currently "
                    f"unprotected — please verify manually."
                )
                continue
        if filled_qty != qty:
            print(f"      [strategy5] {symbol}: requested {qty}, filled {filled_qty} "
                  f"(partial fill) — sizing TP/SL off the filled amount.")
        available_balance_usdt -= order_margin_usdt * (filled_qty / qty)
        qty = filled_qty

        # Track and save IMMEDIATELY on fill confirmation, BEFORE attempting
        # TP/SL — not after, like before. Reason: sys.exit(0) inside
        # _handle_shutdown_signal() (fired on SIGTERM, e.g. a Railway
        # redeploy — very possible mid-cycle, including right now while
        # deploying earlier fixes) raises SystemExit, which is a
        # BaseException, NOT an Exception — every try/except Exception guard
        # added above (entry order, TP, SL) does NOT catch it. If that signal
        # lands anywhere between the fill confirming and the old tracking
        # code at the bottom of this function, the position — already real
        # and live on the exchange, possibly with a real TP already resting
        # too — was lost entirely: untracked, no /status entry, nothing in
        # Telegram. Seen live on an EIGENUSDT long. Saving the position here,
        # BEFORE either order-placement network call below, shrinks that
        # unsafe window down to a few lines of pure in-memory dict work with
        # no network I/O in between — about as close to zero as this can get
        # without a much bigger redesign. TP/SL details get filled in and
        # re-saved below once/if those calls succeed.
        with state_lock:
            open_shorts[symbol] = {
                "entry_price": entry_price,
                "qty": qty,
                "tp_price": None,
                "tp_order_id": None,
                "sl_price": None,
                "sl_order_id": None,
                "price_precision": price_precision,
                "opened_at_ms": opened_at_ms,
                "simulated": DRY_RUN,
                "leverage": leverage,
                "liquidation_warning_sent": False,
                "side": side,
                "strategy": "5",
            }
            daily_trade_tracker["recent_entries"][symbol] = opened_at_ms
            save_state(open_shorts, daily_trade_tracker)

        # Take-profit AND stop-loss are both flat STRATEGY5_TP_PCT/STRATEGY5_SL_PCT
        # price moves off entry (not %-of-capital figures) — direction-aware,
        # same shape as strategy 4's TP but with a symmetric SL added, matching
        # the backtest's "closes on whichever of TP/SL hits first" rule.
        tp_pct = STRATEGY5_TP_PCT / 100
        sl_pct = STRATEGY5_SL_PCT / 100
        if side == "LONG":
            tp_price = round(entry_price * (1 + tp_pct), price_precision)
            sl_price = round(entry_price * (1 - sl_pct), price_precision) if STRATEGY5_SL_PCT > 0 else None
            close_side = "SELL"
        else:
            tp_price = round(entry_price * (1 - tp_pct), price_precision)
            sl_price = round(entry_price * (1 + sl_pct), price_precision) if STRATEGY5_SL_PCT > 0 else None
            close_side = "BUY"

        try:
            tp_resp = place_order(symbol, side=close_side, order_type="LIMIT",
                                   quantity=qty, price=tp_price, reduce_only=True)
            tp_order_id = tp_resp["data"].get("order_id")
            print(f"      [strategy5] take-profit @ {tp_price} ({STRATEGY5_TP_PCT:g}% price move): "
                  f"{tp_resp['data']}")
            with state_lock:
                if symbol in open_shorts:  # still there unless closed/edited in the meantime
                    open_shorts[symbol]["tp_price"] = tp_price
                    open_shorts[symbol]["tp_order_id"] = tp_order_id
                    save_state(open_shorts, daily_trade_tracker)
        except Exception as e:
            tp_order_id = None
            tp_price = None
            print(f"      [strategy5] {symbol}: failed to place take-profit order ({e}) — "
                  f"position will run with no take-profit until you set one manually via /tp or /tppct.")
            send_telegram_message(
                f"⚠️ [Strategy 5] {symbol} take-profit order failed to place: {e}. "
                f"Position is open with NO take-profit — use /tp {symbol} PRICE or "
                f"/tppct {symbol} PERCENT to set one manually."
            )

        sl_order_id = None
        if sl_price is not None:
            try:
                sl_resp = place_order(symbol, side=close_side, order_type="STOP_MARKET",
                                       quantity=qty, trigger_price=sl_price, reduce_only=True)
                sl_order_id = sl_resp["data"].get("order_id")
                print(f"      [strategy5] stop-loss @ {sl_price} ({STRATEGY5_SL_PCT:g}% price move): "
                      f"{sl_resp['data']}")
                with state_lock:
                    if symbol in open_shorts:
                        open_shorts[symbol]["sl_price"] = sl_price
                        open_shorts[symbol]["sl_order_id"] = sl_order_id
                        save_state(open_shorts, daily_trade_tracker)
            except Exception as e:
                # Don't abort the whole entry over a failed SL placement — the
                # position is already open (and already tracked) with a real
                # take-profit resting. Flag it loudly instead so it doesn't
                # silently run without a stop-loss.
                print(f"      [strategy5] {symbol}: failed to place stop-loss order ({e}) — "
                      f"position will run with take-profit only until you set one manually via /sl.")
                send_telegram_message(
                    f"⚠️ [Strategy 5] {symbol} stop-loss order failed to place: {e}. "
                    f"Position is open with take-profit only — use /sl {symbol} PRICE to set one manually."
                )

        entry_msg = (
            f"{'[DRY RUN] ' if DRY_RUN else ''}[Strategy 5 — RE] {side} {symbol}\n"
            f"Entry: {entry_price} (market)  |  Qty: {qty}  |  Leverage: {leverage}x"
            f"{f' ({STRATEGY5_LEVERAGE}x unavailable, capped down)' if leverage < STRATEGY5_LEVERAGE else ''}"
            f"{f' (symbol minimum forced leverage UP from {STRATEGY5_LEVERAGE}x)' if leverage > STRATEGY5_LEVERAGE else ''}\n"
            f"Signal: EMA{STRATEGY5_EMA_FAST}/EMA{STRATEGY5_EMA_SLOW} crossover on "
            f"{STRATEGY5_KLINE_INTERVAL}m candles, confirmed by close\n"
            f"{'No take-profit set' if tp_price is None else f'Take-profit @ {tp_price} ({STRATEGY5_TP_PCT:g}%)'}"
            + (f"  |  Stop-loss @ {sl_price} ({STRATEGY5_SL_PCT:g}%)" if sl_price is not None else "  |  No stop-loss set")
            + "\nCloses on WHICHEVER of TP/SL hits first — no signal-reversal exit for this strategy."
        )
        send_telegram_message(entry_msg)

    return available_balance_usdt


def run_once(instruments, top_cap_symbols, usdt_inr_rate, open_shorts, daily_trade_tracker,
             last_market_refresh_date, last_status_update_ms):
    try:
        tickers = get_all_tickers()
    except Exception as e:
        record_fetch_failure("scan cycle", e)
        raise
    record_fetch_success()
    # Everything in this block reads and/or mutates open_shorts /
    # daily_trade_tracker, the same state telegram_polling_loop() touches the
    # instant a "Close" button is tapped — held under state_lock so a manual
    # close can't interleave mid-reconcile and corrupt the shared dicts.
    with state_lock:
        reconcile_open_shorts(open_shorts, tickers, daily_trade_tracker)

        # Strategy 4's EMA9-reversal exit — runs every cycle regardless of
        # which strategy is currently ACTIVE for new entries, same as
        # reconcile_open_shorts() just above (see check_strategy4_signal_
        # exits()'s docstring). No-op if there's no open Strategy 4 position.
        check_strategy4_signal_exits(open_shorts, daily_trade_tracker)

        # Liquidation-distance check runs every cycle (not on the 15-minute
        # status timer) since an adverse move can cross the warning threshold
        # well before the next scheduled status update.
        if check_liquidation_warnings(open_shorts, tickers):
            save_state(open_shorts, daily_trade_tracker)

        # Same reasoning as the liquidation check above — a position can
        # cross the loss threshold well before the next scheduled status
        # update, so this also runs every cycle rather than on the 15-minute
        # timer.
        if check_loss_warnings(open_shorts, tickers):
            save_state(open_shorts, daily_trade_tracker)

        now_ms = int(time.time() * 1000)
        if now_ms - last_status_update_ms >= STATUS_UPDATE_INTERVAL_SECONDS * 1000:
            send_position_status_update(open_shorts, tickers)
            last_status_update_ms = now_ms

    today = today_ist()

    # Refresh the top-200 market-cap exclusion list and the USDT/INR
    # conversion rate once per IST calendar day. These were previously only
    # ever fetched once at process startup and then reused for the entire
    # lifetime of the container — on Railway that can mean running for days
    # against a market-cap ranking and FX rate that are stale by then. A coin
    # that's fallen out of (or risen into) the top 200 since startup would be
    # screened against the wrong exclusion list, and the margin-per-trade
    # sizing (CAPITAL_INR / usdt_inr_rate) would silently drift from its
    # intended INR value as the real USDT/INR rate moves.
    # Which strategy is active governs whether the top-200 scan is even
    # needed — read it here too (not just after screening below) since
    # strategy 4/5 never consult top_cap_symbols (see the "No market-wide
    # screening at all" branches further down).
    refresh_active_strategy = strategy_state.get("active", ACTIVE_STRATEGY_DEFAULT)
    needs_market_cap_scan = refresh_active_strategy in ("1", "2", "3")
    # Besides the once-a-day refresh, also fire immediately (regardless of
    # last_market_refresh_date) the moment a live /strategy1, /strategy2, or
    # /strategy3 Telegram switch moves INTO a scan-needing strategy while
    # top_cap_symbols is still empty — e.g. because the process started on
    # strategy 4/5 and skipped the scan at startup/last refresh. Without
    # this, switching strategies mid-run could screen candidates against an
    # empty exclusion set until the next calendar-day rollover.
    stale_scan_needed = needs_market_cap_scan and not top_cap_symbols
    if last_market_refresh_date != today or stale_scan_needed:
        try:
            if needs_market_cap_scan:
                top_cap_symbols = get_top_market_cap_symbols(TOP_N_MARKET_CAP_EXCLUDE)
            usdt_inr_rate = get_usdt_inr_rate()
            last_market_refresh_date = today
            if needs_market_cap_scan:
                print(f"  [refresh] top-200 market cap list and USDT/INR rate refreshed for {today} "
                      f"(USDT/INR ~= {usdt_inr_rate}).")
            else:
                print(f"  [refresh] strategy {refresh_active_strategy} active — skipped the top-200 "
                      f"market cap scan (not used by this strategy) and refreshed USDT/INR rate only "
                      f"for {today} (USDT/INR ~= {usdt_inr_rate}).")
        except requests.HTTPError as e:
            # Don't let a transient CoinGecko blip abort this cycle's scan —
            # keep using the previous values and try the refresh again next
            # cycle (last_market_refresh_date is only advanced on success).
            print(f"  [refresh] failed to refresh market cap list / USDT-INR rate ({e}), "
                  f"keeping previous values for this cycle.")

    # Which strategy is actually running — read this FIRST so the balance
    # gate below can be sized off the RIGHT capital figure. Previously this
    # gate always used the strategy 1-3 CAPITAL_INR (10,000 INR) even when
    # strategy 4/5 (which use their own, smaller, per-strategy capital
    # constants) was active, which could silently skip the ENTIRE scan cycle
    # — before enter_trades_strategy4/5() ever ran their own, correct,
    # balance check — whenever the wallet had enough for that strategy's
    # actual trade size but not enough for the unrelated 10,000 INR figure.
    active_strategy = strategy_state.get("active", ACTIVE_STRATEGY_DEFAULT)
    if active_strategy == "4":
        order_margin_usdt = STRATEGY4_CAPITAL_INR / usdt_inr_rate
        gate_required_usdt = order_margin_usdt
        gate_capital_desc = f"{order_margin_usdt:.2f} USDT ({STRATEGY4_CAPITAL_INR:,} INR)"
    elif active_strategy == "5":
        # Strategy 5's minimum wallet requirement is set directly in USDT
        # (STRATEGY5_MIN_TRADE_USDT + STRATEGY5_FEE_BUFFER_USDT), not
        # converted from an INR figure — so it doesn't drift with the live
        # USDT/INR rate. This pre-scan check matches
        # enter_trades_strategy5()'s own per-symbol check exactly.
        order_margin_usdt = STRATEGY5_MIN_TRADE_USDT
        gate_required_usdt = STRATEGY5_MIN_TRADE_USDT + STRATEGY5_FEE_BUFFER_USDT
        gate_capital_desc = (
            f"{gate_required_usdt:.2f} USDT (trade sized {STRATEGY5_MIN_TRADE_USDT:.0f}-"
            f"{STRATEGY5_MAX_TRADE_USDT:.0f} USDT + {STRATEGY5_FEE_BUFFER_USDT:.2f} USDT fee buffer, "
            f"off free balance)"
        )
    else:
        order_margin_usdt = CAPITAL_INR / usdt_inr_rate
        gate_required_usdt = order_margin_usdt
        gate_capital_desc = f"{order_margin_usdt:.2f} USDT ({CAPITAL_INR:,} INR)"

    # Check available balance (USDT + INR, combined at the live rate) BEFORE
    # doing any real work this cycle — including screening candidates. If
    # there isn't enough free margin for even one trade, there's no point
    # scanning the market at all this cycle: just wait for the wallet to be
    # topped up (or for a position to close and free margin) and try again
    # next cycle. This gate is skipped entirely when DRY_RUN is on, so
    # paper-trading can keep scanning/simulating regardless of the real
    # account balance — it's still enforced for live trading, where it now
    # blocks both the search AND the trade, not just the trade.
    try:
        available_balance_usdt = get_wallet_balance(usdt_inr_rate)["total_usdt"]
    except requests.HTTPError as e:
        print(f"  [wallet] balance check failed ({e}), skipping this cycle to be safe.")
        return top_cap_symbols, usdt_inr_rate, last_market_refresh_date, last_status_update_ms

    if not DRY_RUN and available_balance_usdt < gate_required_usdt:
        print(f"  [wallet] available balance {available_balance_usdt:.2f} USDT is below the "
              f"{gate_capital_desc} needed for one trade — "
              f"not scanning for new trades this cycle. Existing open positions are unaffected.")
        return top_cap_symbols, usdt_inr_rate, last_market_refresh_date, last_status_update_ms
    elif DRY_RUN and available_balance_usdt < gate_required_usdt:
        print(f"  [wallet] available balance {available_balance_usdt:.2f} USDT is below the "
              f"{gate_capital_desc} needed for one trade — "
              f"continuing to scan anyway since DRY_RUN is on (no real orders will be placed).")

    if active_strategy in ("1", "3"):
        candidates = screen_candidates(tickers, top_cap_symbols, usdt_inr_rate)
        filter_desc = "market-cap/drop/volume"
    elif active_strategy == "4":
        # No market-wide screening at all — strategy 4 always trades exactly
        # one fixed symbol (see enter_trades_strategy4()).
        candidates = []
        filter_desc = f"{STRATEGY4_SYMBOL}-only EMA9 flip (strategy 4)"
    elif active_strategy == "5":
        # No market-wide screening at all — strategy 5 ("RE Strategy") only
        # trades its fixed symbol list (see enter_trades_strategy5()).
        candidates = []
        filter_desc = f"{', '.join(STRATEGY5_SYMBOLS)}-only EMA9/EMA21 cross (strategy 5, RE Strategy)"
    else:
        candidates = screen_candidates_v2(tickers, top_cap_symbols)
        filter_desc = "market-cap-only (strategy 2)"

    # Reset the daily counters if the calendar day has rolled over (IST).
    # Send yesterday's P&L summary to Telegram before wiping the numbers.
    if daily_trade_tracker["date"] != today:
        with state_lock:
            send_daily_summary(daily_trade_tracker, open_shorts)
            backup_trade_history()
            daily_trade_tracker["date"] = today
            daily_trade_tracker["count"] = 0
            daily_trade_tracker["realized_pnl_usdt"] = 0.0
            daily_trade_tracker["trades_closed"] = 0
            daily_trade_tracker["wins"] = 0
            daily_trade_tracker["losses"] = 0
            save_state(open_shorts, daily_trade_tracker)

    print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] [Strategy {active_strategy} active] "
          f"{len(candidates)} symbol(s) pass the {filter_desc} filter. "
          f"Trades today: {daily_trade_tracker['count']}"
          f"{f'/{MAX_TRADES_PER_DAY}' if not DRY_RUN else ' (no daily cap in DRY_RUN)'}")

    if bot_paused.is_set():
        # Paused via /pause — skip opening new trades entirely this cycle,
        # but everything above (reconcile, liquidation checks, status
        # updates, daily rollover) still ran normally, and manual closes via
        # Telegram still work independently on their own thread.
        if candidates:
            print(f"  [paused] {len(candidates)} candidate(s) passed screening but the bot is "
                  f"paused (/resume to re-enable) — skipping new entries.")
        return top_cap_symbols, usdt_inr_rate, last_market_refresh_date, last_status_update_ms

    # Drop cooldown entries older than the window so this dict doesn't grow
    # forever across a long-running process — done once per cycle rather than
    # per-candidate since it's the same cutoff for every symbol.
    cooldown_cutoff_ms = now_ms - ENTRY_COOLDOWN_MS
    daily_trade_tracker["recent_entries"] = {
        s: t for s, t in daily_trade_tracker["recent_entries"].items() if t >= cooldown_cutoff_ms
    }
    loss_cooldown_cutoff_ms = now_ms - LOSS_COOLDOWN_MS
    daily_trade_tracker["recent_losses"] = {
        s: t for s, t in daily_trade_tracker.get("recent_losses", {}).items() if t >= loss_cooldown_cutoff_ms
    }
    # Same pruning for strategy 5's own re-entry cooldown (see
    # record_recent_close() / STRATEGY5_REENTRY_COOLDOWN_HOURS).
    close_cooldown_cutoff_ms = now_ms - STRATEGY5_REENTRY_COOLDOWN_MS
    daily_trade_tracker["recent_closes"] = {
        s: t for s, t in daily_trade_tracker.get("recent_closes", {}).items() if t >= close_cooldown_cutoff_ms
    }

    # Dispatch to whichever strategy is currently active. Positions already
    # open from the OTHER strategy are unaffected either way — they keep
    # being reconciled/monitored/closeable above and earlier in this
    # function regardless of which strategy is opening new trades right now.
    if active_strategy == "1":
        available_balance_usdt = enter_trades_strategy1(
            candidates, instruments, order_margin_usdt, available_balance_usdt,
            daily_trade_tracker, open_shorts, cooldown_cutoff_ms, now_ms,
            loss_cooldown_cutoff_ms=loss_cooldown_cutoff_ms,
        )
    elif active_strategy == "3":
        available_balance_usdt = enter_trades_strategy3(
            candidates, instruments, order_margin_usdt, available_balance_usdt,
            daily_trade_tracker, open_shorts, cooldown_cutoff_ms, now_ms,
            loss_cooldown_cutoff_ms=loss_cooldown_cutoff_ms,
        )
    elif active_strategy == "4":
        available_balance_usdt = enter_trades_strategy4(
            instruments, usdt_inr_rate, available_balance_usdt,
            daily_trade_tracker, open_shorts, now_ms,
        )
    elif active_strategy == "5":
        available_balance_usdt = enter_trades_strategy5(
            instruments, usdt_inr_rate, available_balance_usdt,
            daily_trade_tracker, open_shorts, now_ms,
        )
    else:
        available_balance_usdt = enter_trades_strategy2(
            candidates, instruments, order_margin_usdt, available_balance_usdt,
            daily_trade_tracker, open_shorts, cooldown_cutoff_ms, now_ms,
            loss_cooldown_cutoff_ms=loss_cooldown_cutoff_ms,
        )
    # Returned so main()'s loop can carry the (possibly refreshed) market
    # data and refresh-date marker into the next cycle — run_once() itself
    # is stateless between calls otherwise.
    return top_cap_symbols, usdt_inr_rate, last_market_refresh_date, last_status_update_ms


def main():
    # Restore whichever strategy was active before this deploy/restart FIRST
    # — before the top-200 CoinGecko market-cap scan below — so that scan
    # can be skipped entirely when strategy 4/5 (which never use
    # top_cap_symbols; see run_once()'s "No market-wide screening at all"
    # branches) is what's actually running. Without this, every redeploy
    # scanned the full top-200 market unconditionally, since strategy_state
    # still held only its ACTIVE_STRATEGY env-var default at this point.
    restore_active_strategy_from_state()
    active_strategy_at_startup = strategy_state.get("active", ACTIVE_STRATEGY_DEFAULT)

    if active_strategy_at_startup in ("1", "2", "3"):
        print("Fetching top-200 market cap list and USDT/INR rate from CoinGecko...")
        top_cap_symbols = fetch_with_retry(
            get_top_market_cap_symbols, TOP_N_MARKET_CAP_EXCLUDE, description="top-200 market cap list"
        )
    else:
        # Strategy 4/5 trade a fixed symbol list and never consult
        # top_cap_symbols — skip the full market scan on this deploy.
        print(f"Strategy {active_strategy_at_startup} active — skipping the top-200 market cap "
              f"scan (not used by this strategy) and fetching USDT/INR rate only...")
        top_cap_symbols = set()
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
        "recent_entries": {},     # symbol -> opened_at_ms of its most recent entry (real or DRY_RUN),
                                   # used for the no-re-entry cooldown (ENTRY_COOLDOWN_HOURS). Rolling window, NOT reset
                                   # on the midnight-IST rollover below (see recover_open_positions()).
        "recent_losses": {},      # symbol -> closed_at_ms of its most recent LOSING close, used for the
                                   # longer no-re-entry cooldown (LOSS_COOLDOWN_HOURS). Same rolling-window
                                   # behavior as recent_entries — survives the midnight-IST rollover.
        "recent_closes": {},      # symbol -> closed_at_ms of its most recent close (ANY reason, win or
                                   # loss), used only by strategy 5's own STRATEGY5_REENTRY_COOLDOWN_HOURS
                                   # gate. Same rolling-window behavior as recent_entries/recent_losses.
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

    # Wire up graceful shutdown now that open_shorts/daily_trade_tracker exist
    # — SIGTERM (Railway redeploy/restart/stop) and SIGINT (Ctrl+C locally)
    # both save state before the process actually exits. See
    # _handle_shutdown_signal()'s docstring for what this does and doesn't do.
    _shutdown_context["open_shorts"] = open_shorts
    _shutdown_context["daily_trade_tracker"] = daily_trade_tracker
    signal.signal(signal.SIGTERM, _handle_shutdown_signal)
    signal.signal(signal.SIGINT, _handle_shutdown_signal)

    # Seeded to 0 (not now_ms) so the very first cycle sends an immediate
    # status update if anything got recovered above, instead of waiting a
    # full 15 minutes after every restart before the first snapshot.
    last_status_update_ms = 0

    trade_cap_desc = f"max {MAX_TRADES_PER_DAY} trades/day" if not DRY_RUN else "no daily trade cap (DRY_RUN)"
    print(f"DRY_RUN = {DRY_RUN}. Active strategy: {strategy_state.get('active', ACTIVE_STRATEGY_DEFAULT)}. "
          f"{trade_cap_desc}. "
          f"Starting scan loop every {POLL_INTERVAL_SECONDS}s. Ctrl+C to stop.")
    send_telegram_message(
        f"{'[DRY RUN] ' if DRY_RUN else ''}Bot started. "
        f"Active strategy: {strategy_state.get('active', ACTIVE_STRATEGY_DEFAULT)} "
        f"({STRATEGY_NAMES[strategy_state.get('active', ACTIVE_STRATEGY_DEFAULT)]}).\n"
        f"Scanning every {POLL_INTERVAL_SECONDS}s, {trade_cap_desc}. "
        f"Loss/liquidation prices re-checked every {PRICE_MONITOR_INTERVAL_SECONDS}s. "
        f"Heartbeat every {format_duration(HEARTBEAT_INTERVAL_SECONDS)}.\n"
        f"Tap ❌ Close under any position in a status update to close it instantly.\n"
        f"Send /status any time for an on-demand snapshot, /history for closed trades, "
        f"/analytics for win rate/profit factor/drawdown stats, "
        f"/cooldowns to see symbols on the {ENTRY_COOLDOWN_HOURS}h re-entry cooldown, "
        f"/debugvolume SYMBOL to inspect raw kline volume data, "
        f"/tp SYMBOL PRICE or /sl SYMBOL PRICE to change a position's take-profit or stop-loss (0 removes it), "
        f"/strategy to check the active strategy, /strategy1, /strategy2, /strategy3, /strategy4, "
        f"or /strategy5 to switch, "
        f"/pause to stop new entries, /resume to re-enable them."
    )

    # Runs the whole time the process is up, independent of the 5-minute scan
    # cycle above — this is what makes a "❌ Close" button tap in Telegram take
    # effect within a second or two instead of waiting for the next scan.
    # Daemon=True so it never blocks process shutdown on its own.
    telegram_thread = threading.Thread(
        target=telegram_polling_loop, args=(open_shorts, daily_trade_tracker), daemon=True
    )
    telegram_thread.start()

    # Fast, independent loss/liquidation monitor — see price_monitor_loop()'s
    # docstring for why this is safe to run alongside the scan loop and the
    # Telegram polling thread above.
    price_monitor_thread = threading.Thread(
        target=price_monitor_loop, args=(open_shorts, daily_trade_tracker), daemon=True
    )
    price_monitor_thread.start()

    # Plain keep-alive ping, independent of everything else — see
    # heartbeat_loop()'s docstring.
    heartbeat_thread = threading.Thread(
        target=heartbeat_loop, args=(open_shorts,), daemon=True
    )
    heartbeat_thread.start()

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
