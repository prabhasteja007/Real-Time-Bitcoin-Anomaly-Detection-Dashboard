import json
import time
import traceback
from datetime import datetime

import websocket
from kafka import KafkaProducer


class CoinbaseProducer:
    def __init__(self):
        print("🔄 Connecting to Kafka...")
        self.producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.messages_sent = 0
        self.last_price = None  # for % change calculation
        print("✅ Connected to Kafka! Using COINBASE BTC-USD stream.\n")

    # ------------- WebSocket callbacks -------------

    def on_open(self, ws):
        print("🚀 Connected to Coinbase WebSocket, subscribing to BTC-USD ticker...\n")
        subscribe_msg = {
            "type": "subscribe",
            "channels": [
                {
                    "name": "ticker",
                    "product_ids": ["BTC-USD"],
                }
            ],
        }
        ws.send(json.dumps(subscribe_msg))

    def on_message(self, ws, message):
        try:
            msg = json.loads(message)

            # We only care about ticker messages
            if msg.get("type") != "ticker":
                return

            # Coinbase ticker message should contain "price"
            price_str = msg.get("price")
            if price_str is None:
                return

            price = float(price_str)

            # Compute % change vs last seen price (simple proxy)
            if self.last_price is None or self.last_price == 0:
                change_pct = 0.0
            else:
                change_pct = (price - self.last_price) / self.last_price * 100.0

            self.last_price = price

            tick = {
                "timestamp": datetime.now().isoformat(),
                "price": round(price, 2),
                "change": round(change_pct, 2),  # this is what Spark uses
            }

            self.producer.send("bitcoin-prices", value=tick)
            self.messages_sent += 1

            if self.messages_sent % 5 == 0:
                print(
                    f"📨 {self.messages_sent:5d} | "
                    f"BTC: ${tick['price']:,.2f} | "
                    f"{tick['change']:+.2f}%"
                )

        except Exception as e:
            print("❌ Error in on_message():", e)
            traceback.print_exc()

    def on_error(self, ws, error):
        print("⚠️ WebSocket error:", error)

    def on_close(self, ws, close_status_code, close_msg):
        print(f"🔚 WebSocket closed — code={close_status_code}, msg={close_msg}")

    # ------------- Runner -------------

    def start(self):
        # Enable this if you want super detailed WebSocket logs:
        # websocket.enableTrace(True)

        ws = websocket.WebSocketApp(
            "wss://ws-feed.exchange.coinbase.com",
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )

        print("🌐 Connecting to Coinbase WebSocket feed...\n")

        try:
            ws.run_forever()
        except KeyboardInterrupt:
            print("⛔ Stopped by user. Total messages sent:", self.messages_sent)
        except Exception as e:
            print("❌ Exception in run_forever():", e)
            traceback.print_exc()


if __name__ == "__main__":
    # Tiny delay in case Kafka just started
    time.sleep(2)
    CoinbaseProducer().start()
