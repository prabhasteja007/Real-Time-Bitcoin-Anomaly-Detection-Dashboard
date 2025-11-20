import json
import time
import os
from collections import Counter
from datetime import datetime

# Path to anomalies.json (written by consumer)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ANOMALY_FILE = os.path.join(BASE_DIR, "anomalies.json")


def clear_screen():
    # Works on Windows and Unix
    os.system("cls" if os.name == "nt" else "clear")


def load_anomalies(max_lines=50):
    """
    Load up to the last `max_lines` anomalies from the JSONL file.
    Each line is a JSON object written by consumer.py.
    """
    if not os.path.exists(ANOMALY_FILE):
        return []

    try:
        with open(ANOMALY_FILE, "r") as f:
            lines = f.readlines()
    except Exception:
        return []

    lines = lines[-max_lines:]
    anomalies = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            a = json.loads(line)
            anomalies.append(a)
        except json.JSONDecodeError:
            continue
    return anomalies


def classify_severity(types):
    """
    Simple severity model:
      - HIGH: pct_spike or abs_jump
      - MED : vol_breakout
      - LOW : mean_reversion_break only
    """
    tset = set(types or [])
    if "pct_spike" in tset or "abs_jump" in tset:
        return "HIGH"
    if "vol_breakout" in tset:
        return "MED"
    if "mean_reversion_break" in tset:
        return "LOW"
    return "LOW"


def show():
    clear_screen()
    print("=" * 90)
    print("  🔍 BITCOIN ANOMALY DETECTION DASHBOARD")
    print("=" * 90)

    anomalies = load_anomalies(max_lines=50)
    total = len(anomalies)

    if total == 0:
        print("\nWaiting for data... (no anomalies yet)\n")
        print("=" * 90)
        return

    # Summary stats
    type_counter = Counter()
    for a in anomalies:
        for t in a.get("types", []):
            type_counter[t] += 1

    last_ts = anomalies[-1].get("timestamp", "")

    print(f"\n📊 Total anomalies (last {total}): {total}")
    if last_ts:
        print(f"🕒 Last anomaly at: {last_ts[:19]}")
    print("📌 Type counts:")
    for t in ["pct_spike", "abs_jump", "vol_breakout", "mean_reversion_break"]:
        if type_counter[t]:
            print(f"   • {t:20s}: {type_counter[t]}")

    print("\n🚨 RECENT ALERTS (most recent first):")
    print("-" * 90)
    print(f"{'Time':19s}  {'Price ($)':>12s}  {'Δ%':>10s}  {'Δ$':>10s}  {'Severity':>8s}  Types")
    print("-" * 90)

    # Show last 15 anomalies, newest first
    for a in reversed(anomalies[-15:]):
        ts = (a.get("timestamp") or "")[:19]
        price = a.get("price")
        pct = a.get("pct_change")
        abs_ch = a.get("abs_change")
        types = a.get("types", [])
        sev = classify_severity(types)

        # Safe formatting
        price_str = f"{price:,.2f}" if isinstance(price, (int, float)) else "n/a"
        pct_str = f"{pct:+.4f}%" if isinstance(pct, (int, float)) else "n/a"
        abs_str = f"{abs_ch:+.2f}" if isinstance(abs_ch, (int, float)) else "n/a"
        types_str = ",".join(types) if types else "-"

        print(f"{ts:19s}  {price_str:>12s}  {pct_str:>10s}  {abs_str:>10s}  {sev:>8s}  {types_str}")

    print("\n" + "=" * 90)


if __name__ == "__main__":
    try:
        while True:
            show()
            time.sleep(3)
    except KeyboardInterrupt:
        print("\nExiting dashboard...")
