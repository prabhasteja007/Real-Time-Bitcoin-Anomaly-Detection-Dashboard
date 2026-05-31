"""
collect_data.py — Fetch historical BTC price data for offline model training.
Uses Kraken public REST API (no API key required, works in the US).

Fetches 1-minute OHLC close prices.
Saves raw price array to prices.npy.

Usage:
    python collect_data.py
    python collect_data.py --bars 5000
"""

import os
import sys
import time
import argparse

import numpy as np
import requests

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, "prices.npy")

# Kraken OHLC endpoint — returns up to 720 bars per request
KRAKEN_OHLC = "https://api.kraken.com/0/public/OHLC"
PAIR        = "XBTUSD"
INTERVAL    = 1      # 1-minute bars
MAX_PER_REQ = 720    # Kraken max per request


def fetch_ohlc(since: int | None = None) -> list:
    params = {"pair": PAIR, "interval": INTERVAL}
    if since:
        params["since"] = since
    resp = requests.get(KRAKEN_OHLC, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if data.get("error"):
        raise ValueError(f"Kraken API error: {data['error']}")
    # Result key varies (e.g. "XXBTZUSD")
    result_key = [k for k in data["result"] if k != "last"][0]
    return data["result"][result_key], data["result"]["last"]


def collect(target: int = 3000) -> np.ndarray:
    """
    Fetch `target` 1-minute close prices by paginating backwards.
    Kraken OHLC format: [time, open, high, low, CLOSE, vwap, volume, count]
    """
    all_prices = []
    since      = None

    print(f"Fetching {target} BTC 1-minute bars from Kraken…")

    while len(all_prices) < target:
        try:
            bars, last = fetch_ohlc(since)
        except requests.RequestException as e:
            print(f"  Request failed: {e} — retrying in 5s…")
            time.sleep(5)
            continue

        if not bars:
            print("  No more data from Kraken.")
            break

        prices = [float(b[4]) for b in bars]   # close price

        # Kraken paginates forward; collect and then trim to target
        all_prices.extend(prices)
        since = int(bars[-1][0]) + 1   # next page starts after last bar
        print(f"  Fetched {len(bars):,} bars — total so far: {len(all_prices):,}")

        if len(bars) < MAX_PER_REQ:
            break   # reached present time

        time.sleep(0.5)

    arr = np.array(all_prices[-target:])
    return arr


def main():
    parser = argparse.ArgumentParser(description="Collect BTC price data for training.")
    parser.add_argument("--bars", type=int, default=3000,
                        help="Number of 1-minute bars to fetch (default: 3000 ≈ 2 days)")
    args = parser.parse_args()

    prices = collect(target=args.bars)

    if len(prices) == 0:
        print("ERROR: No data collected. Check your internet connection.")
        sys.exit(1)

    np.save(OUTPUT_FILE, prices)
    print(f"\nSaved {len(prices):,} prices → {OUTPUT_FILE}")
    print(f"  Min: ${prices.min():,.2f}  |  Max: ${prices.max():,.2f}  |  Mean: ${prices.mean():,.2f}")


if __name__ == "__main__":
    main()
