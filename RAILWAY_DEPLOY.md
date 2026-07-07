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

The script will fail fast with a clear error message on startup if
`COINSWITCH_API_KEY` or `COINSWITCH_SECRET_KEY` are missing — check the
Railway logs if it doesn't stay running.

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
