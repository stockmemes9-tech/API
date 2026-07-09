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
EXCHANGE = "EXCHANGE_2"


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


def main():
    print("Calling CoinSwitch Get Wallet Balance endpoint...")
    headers, path = sign_request("GET", "/trade/api/v2/futures/wallet_balance")

    r = requests.get(BASE_URL + path, headers=headers, timeout=15)

    print(f"HTTP status: {r.status_code}")
    print("Raw response body:")
    body = r.json()
    print(json.dumps(body, indent=2))

    if r.status_code != 200:
        print("\nNon-200 response -- see the raw body above.")
        sys.exit(1)

    print("\n--- Attempting to parse USDT balance the way the bot does ---")
    try:
        base_asset_balances = body["data"]["base_asset_balances"]
    except (KeyError, TypeError):
        print("Could not find data.base_asset_balances in the response.")
        sys.exit(1)

    found = False
    for entry in base_asset_balances:
        if entry.get("base_asset") == "USDT":
            found = True
            try:
                available = float(entry["balances"]["total_available_balance"])
                print(f"USDT available balance: {available} USDT")
            except (KeyError, ValueError, TypeError):
                print(f"Found a USDT entry but couldn't read total_available_balance. Raw: {entry}")
            break

    if not found:
        print("No USDT entry found in base_asset_balances -- futures wallet may have no USDT yet.")

    print("\n--- Checking for an INR balance in the same wallet ---")
    inr_found = False
    for entry in base_asset_balances:
        if entry.get("base_asset") == "INR":
            inr_found = True
            try:
                available = float(entry["balances"]["total_available_balance"])
                print(f"INR available balance: {available} INR")
                print("(the bot converts this to its USDT equivalent at the live rate "
                      "and adds it to the USDT balance above when checking available margin)")
            except (KeyError, ValueError, TypeError):
                print(f"Found an INR entry but couldn't read total_available_balance. Raw: {entry}")
            break
    if not inr_found:
        print("No INR entry found in base_asset_balances.")


if __name__ == "__main__":
    main()
