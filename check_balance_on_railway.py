import os
import sys
import time
import json
import urllib.parse
import requests
from cryptography.hazmat.primitives.asymmetric import ed25519


def require_env(name):
    value = os.environ.get(name)
    if not value:
        print(f"ERROR: required environment variable {name} is not set.")
        sys.exit(1)
    return value


API_KEY = require_env("COINSWITCH_API_KEY")
SECRET_KEY = require_env("COINSWITCH_SECRET_KEY")
BASE_URL = "https://coinswitch.co"


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


def check_once():
    print("Calling CoinSwitch Get Wallet Balance endpoint...", flush=True)
    headers, path = sign_request("GET", "/trade/api/v2/futures/wallet_balance")
    r = requests.get(BASE_URL + path, headers=headers, timeout=15)

    print(f"HTTP status: {r.status_code}", flush=True)
    print("Raw response body:", flush=True)
    body = r.json()
    print(json.dumps(body, indent=2), flush=True)

    if r.status_code != 200:
        print("\nNon-200 response -- see the raw body above.", flush=True)
        return

    print("\n--- Scanning base_asset_balances for any non-USDT entries ---", flush=True)
    try:
        base_asset_balances = body["data"]["base_asset_balances"]
    except (KeyError, TypeError):
        print("Could not find data.base_asset_balances in the response.", flush=True)
        return

    for entry in base_asset_balances:
        asset = entry.get("base_asset")
        print(f"  found entry: base_asset={asset}, balances={entry.get('balances')}", flush=True)

    assets_seen = {e.get("base_asset") for e in base_asset_balances}
    if "INR" in assets_seen:
        print("\n>>> An INR entry WAS found in base_asset_balances.", flush=True)
    else:
        print(f"\n>>> No INR entry found. Assets present: {assets_seen or 'none'}.", flush=True)


def main():
    check_once()
    # Railway's restart policy will re-run this process if it exits, which
    # would spam the logs by repeating the check forever. Sleep indefinitely
    # after the one check so the deployment log shows the result exactly once.
    print("\nDone. Sleeping so this process doesn't restart-loop on Railway. "
          "You can stop/redeploy once you've read the output above.", flush=True)
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
