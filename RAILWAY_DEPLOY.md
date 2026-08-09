# Deploying to Railway

This bot now reads all secrets and settings from environment variables — nothing
is hardcoded in the script anymore.

## 1. Push these three files to a GitHub repo (or a new Railway project via CLI):
- `coinswitch_resistance_short_bot.py`
- `Procfile`
- `requirements.txt`

## 2. Create a new Railway project from that repo
Railway will detect `requirements.txt` and `Procfile` automatically and deploy
it as a **worker** process (not a web service — it doesn't listen on a port,
so don't add a public domain for it).

## 3. Set environment variables
In Railway: your project -> **Variables** tab -> add each of these:

| Variable | Required | Notes |
|---|---|---|
| `COINSWITCH_API_KEY` | yes | from CoinSwitch PRO > API Trading |
| `COINSWITCH_SECRET_KEY` | yes | from CoinSwitch PRO > API Trading |
| `DRY_RUN` | no (defaults to `true`) | set to `false` only once you trust the signals |
| `TELEGRAM_BOT_TOKEN` | no, but needed for alerts | from BotFather |
| `TELEGRAM_CHAT_ID` | no, but needed for alerts | from the getUpdates JSON |
| `ENABLE_TELEGRAM_NOTIFICATIONS` | no (defaults to `true`) | set `false` to silence without unsetting the token |
| `ACTIVE_STRATEGY` | no (defaults to `1`) | which strategy (`1`-`5`) a **fresh** deploy starts on — see the volume section below for why this matters |
| `STATE_FILE_PATH` | no (defaults to `bot_state.json`) | set to a path on your mounted volume, e.g. `/data/bot_state.json` — see below |
| `TRADE_HISTORY_FILE_PATH` | no (defaults to `trade_history.csv`) | set to a path on your mounted volume, e.g. `/data/trade_history.csv` — see below |

The script will fail fast with a clear error message on startup if
`COINSWITCH_API_KEY` or `COINSWITCH_SECRET_KEY` are missing — check the
Railway logs if it doesn't stay running.

## 3a. Add a persistent volume (strongly recommended)
By default the bot's local state file (open positions bookkeeping, today's
trade counters, the active strategy, and trade history CSV) lives on the
container's local disk. **Railway's local disk is ephemeral** — every
redeploy (any GitHub push) wipes it, since there's no volume mounted by
default. Without a volume:
- Every redeploy silently resets which strategy is active back to
  `ACTIVE_STRATEGY`'s default, even if you'd switched strategies live via
  `/strategy1`-`/strategy5` in Telegram. If that pushes you back onto
  strategy 1/2/3, the bot will also re-run the full top-200 market cap scan
  on that deploy that strategy 4/5 deploys otherwise skip.
- Today's trade count / win-loss / P&L counters reset on every push, not just
  at midnight IST.
- `trade_history.csv` (used by `/history`) is lost on every push.

To fix this properly:
1. Railway -> your service -> **Settings** -> **Volumes** -> add a volume,
   mount it at `/data`.
2. Set `STATE_FILE_PATH=/data/bot_state.json` and
   `TRADE_HISTORY_FILE_PATH=/data/trade_history.csv` in Variables.
3. Redeploy once. From then on, the active strategy, open-position
   bookkeeping, daily counters, and trade history all survive every future
   push/restart.

If you'd rather skip the volume, at minimum keep `ACTIVE_STRATEGY` in
Variables set to whichever strategy you actually want running — that way a
fresh deploy (with no state file to restore from) still starts on the right
strategy instead of falling back to strategy 1's default and re-scanning.

## 4. Watch the logs
Railway's **Deployments -> Logs** tab shows everything the script prints —
same output you were seeing in your local terminal, including each scan
cycle and DRY RUN order simulations. You should also get a "Bot started..."
message in Telegram within a few seconds of deploy if the Telegram vars are
set correctly.

## 5. Rotate your CoinSwitch keys
The keys that were previously hardcoded in the script (and shared in this
chat) should be treated as compromised. Generate a new API key/secret pair
in CoinSwitch PRO, and use the new pair for `COINSWITCH_API_KEY` /
`COINSWITCH_SECRET_KEY` in Railway — don't reuse the old ones.

## Cost note
Railway's free tier has a limited monthly hour allowance that a 24/7 process
will burn through. Check your current plan under Railway -> Usage/Billing;
you'll likely need to be on a usage-based paid plan for this to run
continuously without stopping mid-month.
