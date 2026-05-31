"""
producer.py — Real-Time BTC Price Producer
Streams BTC/USD trades from Kraken WebSocket → Kafka topic.
(Kraken requires no API key and is accessible from the US.)

Config (environment variables):
  KAFKA_BROKER  default: localhost:9092   (use kafka:9092 inside Docker)
  KAFKA_TOPIC   default: bitcoin-prices
"""

import json
import os
import time
import logging
import traceback
from datetime import datetime, timezone

import websocket
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Config ────────────────────────────────────────────────────────
KAFKA_BROKER    = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC",  "bitcoin-prices")
KRAKEN_WS_URL   = "wss://ws.kraken.com"
RECONNECT_DELAY = 5

# Kraken subscription message for trade stream
SUBSCRIBE_MSG = json.dumps({
    "event": "subscribe",
    "pair":  ["XBT/USD"],
    "subscription": {"name": "trade"},
})

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def make_producer(retries: int = 10, delay: int = 3) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            p = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            log.info("Connected to Kafka @ %s", KAFKA_BROKER)
            return p
        except NoBrokersAvailable:
            log.warning("Kafka not ready (%d/%d) — retrying in %ds…", attempt, retries, delay)
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Kafka after {retries} attempts.")


class KrakenProducer:
    def __init__(self):
        self.producer   = make_producer()
        self.sent       = 0
        self.last_price = None

    def on_open(self, ws):
        log.info("WebSocket open → subscribing to XBT/USD trades")
        ws.send(SUBSCRIBE_MSG)

    def on_message(self, ws, raw: str):
        """
        Kraken trade stream format:
          [channelID, [[price, volume, time, side, orderType, misc], ...], "trade", "XBT/USD"]
        We ignore heartbeats and system/subscription events (they are dicts, not lists).
        """
        try:
            msg = json.loads(raw)

            # Skip control messages (dicts) — only process trade arrays
            # Format: [channelID, [[trades...]], "trade", "XBT/USD"]
            if not isinstance(msg, list) or len(msg) < 4 or msg[-2] != "trade":
                return

            trades = msg[1]   # list of trades in this message
            for trade in trades:
                price     = float(trade[0])
                trade_ts  = float(trade[2])   # Unix timestamp from Kraken
                ts        = datetime.fromtimestamp(trade_ts, tz=timezone.utc).isoformat()
                change    = (
                    (price - self.last_price) / self.last_price * 100.0
                    if self.last_price else 0.0
                )
                self.last_price = price

                self.producer.send(KAFKA_TOPIC, value={
                    "timestamp": ts,
                    "price":     round(price, 2),
                    "change":    round(change, 6),
                })
                self.sent += 1

            if self.sent % 20 == 0:
                log.info("Sent %5d | BTC $%-12s | Δ%%=%+.4f", self.sent, f"{price:,.2f}", change)

        except (KeyError, ValueError, IndexError):
            pass
        except Exception:
            log.error("on_message error:\n%s", traceback.format_exc())

    def on_error(self, ws, err):
        log.error("WS error: %s", err)

    def on_close(self, ws, code, msg):
        log.warning("WS closed — code=%s", code)

    def run(self):
        while True:
            ws = websocket.WebSocketApp(
                KRAKEN_WS_URL,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
            )
            try:
                ws.run_forever(ping_interval=20, ping_timeout=10)
            except KeyboardInterrupt:
                log.info("Stopped. Total sent: %d", self.sent)
                self.producer.flush()
                self.producer.close()
                return
            log.info("Reconnecting in %ds…", RECONNECT_DELAY)
            time.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    time.sleep(2)
    KrakenProducer().run()
