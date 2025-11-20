from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import os
import builtins  # to force Python's abs, round, max instead of PySpark versions
import statistics
from collections import deque
from datetime import datetime, timezone


# Schema for the JSON messages coming from Kafka (produced by producer.py)
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("price", DoubleType()),
    StructField("change", DoubleType()),  # not trusted; we recompute deltas ourselves
])


class Detector:
    def __init__(self):
        print("🔄 Starting Spark...")

        self.spark = SparkSession.builder \
            .appName("Crypto-Anomaly-Detection") \
            .master("local[*]") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("ERROR")
        self.count = 0

        # ---- Anomaly log path: C:\603-Project\dashboard\anomalies.json ----
        consumer_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(consumer_dir)
        self.dashboard_dir = os.path.join(project_root, "dashboard")
        os.makedirs(self.dashboard_dir, exist_ok=True)
        self.anomaly_file = os.path.join(self.dashboard_dir, "anomalies.json")

        # ---- State for real-time anomaly detection ----
        self.last_price = None
        self.last_ts = None  # datetime

        # Time window for Δ% and Δ$ comparisons (seconds)
        self.max_seconds_between = 10.0

        # Thresholds for spike & jump anomalies
        self.min_pct_move = 0.25      # 0.25% spike in <= 10s
        self.min_abs_move = 30.0      # $30 jump in <= 10s

        # Rolling window for volatility & mean reversion (recent prices)
        self.vol_window = deque(maxlen=60)  # last 60 ticks for more stable stats

        # Volatility breakout anomaly (based on returns)
        self.vol_k = 3.0                 # k * sigma(returns)
        self.vol_min_abs_move = 15.0     # at least $15 move for a "vol_breakout"

        # Mean reversion anomaly (based on price distance from rolling mean)
        self.mr_k = 2.0                  # 2 * sigma(price)
        self.mr_min_abs_dev = 25.0       # at least $25 away from local mean
        self.in_mr_break = False         # state flag: currently outside mean band?

        print(f"✅ Ready! Logging anomalies to: {self.anomaly_file}")
        print("   Detecting anomaly types:")
        print(f"   • pct_spike        : |Δ%| >= {self.min_pct_move}% within {self.max_seconds_between}s")
        print(f"   • abs_jump         : |Δ$| >= ${self.min_abs_move:.2f} within {self.max_seconds_between}s")
        print(f"   • vol_breakout     : |Δ$| >= {self.vol_k} × σ(returns), floor=${self.vol_min_abs_move:.2f}")
        print(f"   • mean_rev_break   : |P - μ| >= {self.mr_k} × σ(price), floor=${self.mr_min_abs_dev:.2f}\n")

    # ---------- Helper: write anomaly -----------

    def log_anomaly(self, timestamp_str, price, pct_change, abs_change, types, sigma=None):
        """
        Append anomaly as a JSON line and print a human-readable alert.
        types: list like ["pct_spike", "abs_jump", "vol_breakout", "mean_reversion_break"]
        sigma: volatility measure (usually sigma of returns), can be None
        """
        self.count += 1
        alert = {
            "timestamp": timestamp_str,
            "price": float(price) if price is not None else None,
            "pct_change": builtins.round(pct_change, 6) if pct_change is not None else None,
            "abs_change": builtins.round(abs_change, 2) if abs_change is not None else None,
            "types": types,
            "sigma": builtins.round(sigma, 4) if sigma is not None else None
        }

        with open(self.anomaly_file, "a") as f:
            f.write(json.dumps(alert) + "\n")

        types_str = ",".join(types)
        print(
            f"🚨 ANOMALY #{self.count:4d} | {timestamp_str[:19]} | "
            f"${price:,.2f} | Δ%={pct_change:+.4f}% | Δ$={abs_change:+.2f} | types=[{types_str}]"
        )

    # ---------- Foreach-batch logic -----------

    def process(self, df, batch_id):
        if df.isEmpty():
            return

        # Parse JSON from Kafka "value" column
        parsed = df.select(
            from_json(col("value").cast("string"), schema).alias("d")
        ).select("d.*")

        # Process in chronological order by producer timestamp
        rows = parsed.orderBy("timestamp").collect()

        for row in rows:
            ts_str = row["timestamp"]
            price = row["price"]

            if price is None or ts_str is None:
                continue

            # Parse ISO timestamp
            try:
                current_ts = datetime.fromisoformat(ts_str)
                if current_ts.tzinfo is None:
                    current_ts = current_ts.replace(tzinfo=timezone.utc)
            except Exception:
                # Skip bad timestamps
                continue

            pct_change = None
            abs_change = None
            sigma_returns = None
            triggered = []

            # --- Compute deltas vs last tick for spike/jump/volatility ---
            if self.last_price is not None and self.last_ts is not None:
                dt = (current_ts - self.last_ts).total_seconds()

                if 0 < dt <= self.max_seconds_between:
                    abs_change = price - self.last_price
                    pct_change = (price - self.last_price) / self.last_price * 100.0

                    # 1) Percentage spike anomaly
                    if builtins.abs(pct_change) >= self.min_pct_move:
                        triggered.append("pct_spike")

                    # 2) Absolute $ jump anomaly
                    if builtins.abs(abs_change) >= self.min_abs_move:
                        triggered.append("abs_jump")

                    # 3) Volatility breakout anomaly (k * σ on returns)
                    prices_for_sigma = [p for _, p in self.vol_window]
                    if len(prices_for_sigma) >= 10:
                        returns = [
                            prices_for_sigma[i] - prices_for_sigma[i - 1]
                            for i in range(1, len(prices_for_sigma))
                        ]
                        if len(returns) >= 5:
                            sigma_returns = statistics.pstdev(returns)
                            if sigma_returns > 0:
                                vol_threshold = builtins.max(
                                    self.vol_k * sigma_returns,
                                    self.vol_min_abs_move
                                )
                                if builtins.abs(abs_change) >= vol_threshold:
                                    triggered.append("vol_breakout")

            # 4) Mean-reversion break anomaly (distance from rolling mean price),
            #    evaluated as a STATE: we only alert on transition into the break.
            mr_active = False
            prices_for_mean = [p for _, p in self.vol_window]
            if len(prices_for_mean) >= 10:
                mu = statistics.mean(prices_for_mean)
                sigma_price = statistics.pstdev(prices_for_mean)
                if sigma_price > 0:
                    dev = price - mu  # deviation from local mean
                    mr_threshold = builtins.max(
                        self.mr_k * sigma_price,
                        self.mr_min_abs_dev
                    )
                    if builtins.abs(dev) >= mr_threshold:
                        mr_active = True

            # Transition logic: only trigger when we ENTER a mean-reversion break
            if mr_active and not self.in_mr_break:
                triggered.append("mean_reversion_break")
                self.in_mr_break = True
            elif not mr_active and self.in_mr_break:
                # we re-enter the normal band; reset state
                self.in_mr_break = False

            # Log anomaly if any rule fired and we have proper deltas
            if triggered and pct_change is not None and abs_change is not None:
                self.log_anomaly(ts_str, price, pct_change, abs_change, triggered, sigma_returns)

            # Update rolling state (after checks)
            self.last_price = price
            self.last_ts = current_ts
            self.vol_window.append((current_ts, price))

    # ---------- Start streaming -----------

    def start(self):
        df = self.spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "bitcoin-prices") \
            .load()

        df.writeStream \
          .foreachBatch(self.process) \
          .trigger(processingTime="3 seconds") \
          .start() \
          .awaitTermination()


if __name__ == "__main__":
    Detector().start()
