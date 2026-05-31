"""
consumer.py — Real-Time BTC Anomaly Detection Consumer
Reads from Kafka via Spark Structured Streaming.

Detection layers (in order):
  1. Rule-based   : pct_spike, abs_jump, vol_breakout, mean_reversion_break
  2. Isolation Forest (sklearn) — warm-up for first 300 ticks, then always active
  3. LSTM Autoencoder (TensorFlow) — optional, loaded from pre-trained artifacts

Config (environment variables):
  KAFKA_BROKER   default: localhost:9092
  KAFKA_TOPIC    default: bitcoin-prices
  ENABLE_LSTM    default: true   (set to false to skip LSTM)
  ANOMALY_FILE   default: /shared/anomalies.json
  PRICE_FILE     default: /shared/prices_live.json   (live price feed for dashboard)
"""

import os
import json
import time
import statistics
import traceback
from collections import deque
from datetime import datetime, timezone

import numpy as np
import joblib
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ── Config ────────────────────────────────────────────────────────
KAFKA_BROKER  = os.getenv("KAFKA_BROKER",  "localhost:9092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC",   "bitcoin-prices")
ENABLE_LSTM   = os.getenv("ENABLE_LSTM",   "true").lower() == "true"
ANOMALY_FILE  = os.getenv("ANOMALY_FILE",  "/shared/anomalies.json")
PRICE_FILE    = os.getenv("PRICE_FILE",    "/shared/prices_live.json")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Model artifact paths
IF_MODEL_FILE    = os.path.join(BASE_DIR, "iforest_model.save")
IF_SCALER_FILE   = os.path.join(BASE_DIR, "iforest_scaler.save")
LSTM_MODEL_FILE  = os.path.join(BASE_DIR, "lstm_model.h5")
LSTM_SCALER_FILE = os.path.join(BASE_DIR, "lstm_scaler.save")
LSTM_THRESH_FILE = os.path.join(BASE_DIR, "lstm_threshold.npy")

# ── Kafka message schema ──────────────────────────────────────────
SCHEMA = StructType([
    StructField("timestamp", StringType()),
    StructField("price",     DoubleType()),
    StructField("change",    DoubleType()),
])

# ── Severity by anomaly type ──────────────────────────────────────
TYPE_SEVERITY = {
    "pct_spike":            "HIGH",
    "abs_jump":             "HIGH",
    "lstm_anomaly":         "HIGH",
    "vol_breakout":         "MED",
    "iforest_anomaly":      "MED",
    "mean_reversion_break": "LOW",
}
SEV_RANK = {"HIGH": 3, "MED": 2, "LOW": 1}

# How many live price ticks to keep in prices_live.json
MAX_PRICE_HISTORY = 500


def top_severity(types: list) -> str:
    if not types:
        return "LOW"
    return max((TYPE_SEVERITY.get(t, "LOW") for t in types), key=lambda s: SEV_RANK[s])


# ─────────────────────────────────────────────────────────────────

class AnomalyDetector:

    def __init__(self):
        os.makedirs(os.path.dirname(os.path.abspath(ANOMALY_FILE)), exist_ok=True)
        # Fresh session — clear stale data from previous runs
        for f in [ANOMALY_FILE, PRICE_FILE]:
            try:
                open(f, "w").close()
            except Exception:
                pass

        # ── Spark ────────────────────────────────────────────────
        print("Starting Spark…")
        self.spark = (
            SparkSession.builder
            .appName("BTC-Anomaly-Detection")
            .master("local[2]")
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.default.parallelism", "2")
            .config("spark.driver.memory", "1g")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("ERROR")
        print("  Spark ready.")

        # ── Rolling state ────────────────────────────────────────
        self.last_price   = None
        self.last_ts      = None
        self.price_window = deque(maxlen=120)   # (ts, price) tuples
        self.in_mr_break  = False
        self.total_events = 0
        self.total_anomalies = 0
        self.start_time   = time.time()

        # Live price history (written to PRICE_FILE for dashboard)
        self.price_history = deque(maxlen=MAX_PRICE_HISTORY)

        # ── Rule thresholds ──────────────────────────────────────
        self.max_dt    = 10.0   # max seconds between ticks for spike/jump
        self.min_pct   = 0.15   # % threshold -> pct_spike  (lowered for sensitivity)
        self.min_abs   = 20.0   # $ threshold -> abs_jump   (lowered for sensitivity)
        self.vol_k     = 2.5    # sigma multiplier -> vol_breakout
        self.vol_floor = 10.0   # minimum $ move for vol_breakout
        self.mr_k      = 2.0    # sigma multiplier -> mean_reversion_break
        self.mr_floor  = 20.0   # minimum $ deviation for mean_reversion_break

        # ── Isolation Forest state ───────────────────────────────
        self.if_model  = None
        self.if_scaler = None
        self.if_buf    = []
        self.IF_WARMUP  = 300   # ticks before first fit
        self.IF_RETRAIN = 500
        self.if_ticks   = 0
        self._load_if()

        # ── LSTM state ───────────────────────────────────────────
        self.lstm_model  = None
        self.lstm_scaler = None
        self.lstm_thresh = None
        self.lstm_errors = deque(maxlen=300)
        if ENABLE_LSTM:
            self._load_lstm()

        print(f"\n{'='*60}")
        print(f"  BTC Anomaly Detector — online")
        print(f"  Anomaly log  -> {ANOMALY_FILE}")
        print(f"  Price feed   -> {PRICE_FILE}")
        print(f"  Rules        : pct>={self.min_pct}% | abs>=${self.min_abs} | "
              f"vol>={self.vol_k}σ (>=${self.vol_floor}) | mr>={self.mr_k}σ (>=${self.mr_floor})")
        print(f"  IF           : {'pre-trained' if self.if_model else f'warming up ({self.IF_WARMUP} ticks needed)'}")
        print(f"  LSTM         : {'loaded ✓' if self.lstm_model else 'disabled / not found'}")
        print(f"{'='*60}\n")

    # ── Model loading ────────────────────────────────────────────────

    def _load_if(self):
        if os.path.exists(IF_MODEL_FILE) and os.path.exists(IF_SCALER_FILE):
            try:
                self.if_model  = joblib.load(IF_MODEL_FILE)
                self.if_scaler = joblib.load(IF_SCALER_FILE)
                print("  [IF]   Pre-trained model loaded.")
            except Exception as e:
                print(f"  [IF]   Load failed: {e} — will warm-up online.")
        else:
            print(f"  [IF]   No saved model — will train online after {self.IF_WARMUP} ticks.")

    def _load_lstm(self):
        required = [LSTM_MODEL_FILE, LSTM_SCALER_FILE, LSTM_THRESH_FILE]
        if not all(os.path.exists(f) for f in required):
            print("  [LSTM] Artifacts not found. Run train_model.py first.")
            return
        try:
            from tensorflow.keras.models import load_model as tf_load
            self.lstm_model  = tf_load(LSTM_MODEL_FILE, compile=False)
            self.lstm_scaler = joblib.load(LSTM_SCALER_FILE)
            self.lstm_thresh = float(np.load(LSTM_THRESH_FILE))
            print(f"  [LSTM] Loaded. Base threshold={self.lstm_thresh:.8f}")
        except ImportError:
            print("  [LSTM] TensorFlow not installed — LSTM disabled.")
        except Exception as e:
            print(f"  [LSTM] Load failed: {e}")

    # ── Feature extraction ───────────────────────────────────────────

    def _if_features(self, price):
        """5-dim feature vector: [price, r1, r5, rolling_std, z_score]."""
        prices = [p for _, p in self.price_window]
        if len(prices) < 20:
            return None
        seg   = prices[-20:]
        mu    = statistics.mean(seg)
        sigma = statistics.pstdev(seg) or 1e-8
        r1    = (prices[-1] - prices[-2]) / (prices[-2] or 1e-8) if len(prices) >= 2 else 0.0
        r5    = (prices[-1] - prices[-6]) / (prices[-6] or 1e-8) if len(prices) >= 6 else 0.0
        z     = (price - mu) / sigma
        return [price, r1, r5, sigma, z]

    # ── Detection methods ────────────────────────────────────────────

    def _rules(self, pct_change, abs_change):
        triggered = []
        if abs(pct_change) >= self.min_pct:
            triggered.append("pct_spike")
        if abs(abs_change) >= self.min_abs:
            triggered.append("abs_jump")
        return triggered

    def _vol_breakout(self, abs_change):
        prices = [p for _, p in self.price_window]
        if len(prices) < 10:
            return []
        returns = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
        if len(returns) < 5:
            return []
        sigma = statistics.pstdev(returns)
        if sigma > 0 and abs(abs_change) >= max(self.vol_k * sigma, self.vol_floor):
            return ["vol_breakout"]
        return []

    def _mean_reversion(self, price):
        prices = [p for _, p in self.price_window]
        if len(prices) < 10:
            return []
        mu    = statistics.mean(prices)
        sigma = statistics.pstdev(prices)
        if sigma <= 0:
            return []
        if abs(price - mu) >= max(self.mr_k * sigma, self.mr_floor):
            if not self.in_mr_break:
                self.in_mr_break = True
                return ["mean_reversion_break"]
        else:
            self.in_mr_break = False
        return []

    def _iforest_check(self, features):
        if self.if_model is None or features is None:
            return [], 0.0
        try:
            X     = self.if_scaler.transform([features])
            pred  = self.if_model.predict(X)[0]
            score = float(-self.if_model.score_samples(X)[0])
            if pred == -1:
                return ["iforest_anomaly"], score
            return [], score
        except Exception as e:
            print(f"  [IF] Scoring error: {e}")
            return [], 0.0

    def _lstm_check(self):
        if self.lstm_model is None:
            return [], 0.0
        prices = [p for _, p in self.price_window]
        if len(prices) < 60:
            return [], 0.0
        try:
            seq   = np.array(prices[-60:]).reshape(-1, 1)
            seq_s = self.lstm_scaler.transform(seq).reshape(1, 60, 1)
            recon = self.lstm_model.predict(seq_s, verbose=0)
            error = float(np.mean((seq_s - recon) ** 2))
            self.lstm_errors.append(error)
            dyn_thresh = (
                float(np.mean(self.lstm_errors) + 2 * np.std(self.lstm_errors))
                if len(self.lstm_errors) > 50
                else self.lstm_thresh
            )
            score = error / (dyn_thresh or 1e-10)
            if error > dyn_thresh:
                return ["lstm_anomaly"], score
            return [], score
        except Exception as e:
            print(f"  [LSTM] Scoring error: {e}")
            return [], 0.0

    # ── Online IF management ─────────────────────────────────────────

    def _update_iforest(self, features):
        if features is None:
            return
        self.if_buf.append(features)
        self.if_ticks += 1
        first_fit = self.if_model is None and len(self.if_buf) >= self.IF_WARMUP
        retrain   = self.if_model is not None and self.if_ticks >= self.IF_RETRAIN
        if first_fit or retrain:
            self._fit_iforest()

    def _fit_iforest(self):
        X      = np.array(self.if_buf)
        scaler = StandardScaler()
        X_s    = scaler.fit_transform(X)
        model  = IsolationForest(n_estimators=100, contamination=0.01,
                                 random_state=42, n_jobs=-1)
        model.fit(X_s)
        self.if_model  = model
        self.if_scaler = scaler
        self.if_ticks  = 0
        self.if_buf    = self.if_buf[-self.IF_WARMUP:]
        print(f"  [IF] ✓ Fitted on {len(X):,} samples — now active.")

    # ── Write live price feed ────────────────────────────────────────

    def _write_price_feed(self, ts_str, price, if_score, anomaly_types):
        """Append one tick to the live price file for the dashboard."""
        self.price_history.append({
            "timestamp": ts_str,
            "price":     round(price, 2),
            "if_score":  round(if_score, 4),
            "anomaly":   len(anomaly_types) > 0,
            "if_ready":  self.if_model is not None,
        })
        try:
            with open(PRICE_FILE, "w") as f:
                json.dump(list(self.price_history), f)
        except Exception:
            pass

    # ── Logging ──────────────────────────────────────────────────────

    def _log(self, ts_str, price, pct_change, abs_change, types,
             if_score=0.0, lstm_score=0.0):
        self.total_anomalies += 1
        severity = top_severity(types)
        record   = {
            "timestamp":  ts_str,
            "price":      round(price, 2),
            "pct_change": round(pct_change, 6),
            "abs_change": round(abs_change, 2),
            "types":      types,
            "severity":   severity,
            "if_score":   round(if_score,   4),
            "lstm_score": round(lstm_score, 4),
        }
        with open(ANOMALY_FILE, "a") as f:
            f.write(json.dumps(record) + "\n")

        icon = {"HIGH": "🔴", "MED": "🟡", "LOW": "🟢"}.get(severity, "⚪")
        print(
            f"  {icon} #{self.total_anomalies:4d} | {ts_str[11:19]} | "
            f"${price:>10,.2f} | Δ%={pct_change:+.4f}% | [{', '.join(types)}]"
        )

    # ── Spark foreachBatch ───────────────────────────────────────────

    def process(self, df, batch_id):
        if df.isEmpty():
            return

        try:
            rows = (
                df.select(from_json(col("value").cast("string"), SCHEMA).alias("d"))
                  .select("d.*")
                  .orderBy("timestamp")
                  .collect()
            )
        except Exception as e:
            print(f"  [batch {batch_id}] Parse error: {e}")
            return

        for row in rows:
            ts_str = row["timestamp"]
            price  = row["price"]
            if price is None or ts_str is None:
                continue

            try:
                ts = datetime.fromisoformat(ts_str)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
            except Exception:
                continue

            self.total_events += 1
            triggered  = []
            pct_change = 0.0
            abs_change = 0.0
            if_score   = 0.0
            lstm_score = 0.0

            # 1. Rule-based (requires valid delta)
            if self.last_price is not None and self.last_ts is not None:
                dt = (ts - self.last_ts).total_seconds()
                if 0 < dt <= self.max_dt:
                    abs_change = price - self.last_price
                    pct_change = abs_change / self.last_price * 100.0
                    triggered += self._rules(pct_change, abs_change)
                    triggered += self._vol_breakout(abs_change)

            # 2. Mean reversion (uses existing window before update)
            triggered += self._mean_reversion(price)

            # Update rolling state
            self.price_window.append((ts, price))
            self.last_price = price
            self.last_ts    = ts

            # 3. Isolation Forest
            features = self._if_features(price)
            self._update_iforest(features)
            if_types, if_score = self._iforest_check(features)
            triggered += if_types

            # 4. LSTM
            lstm_types, lstm_score = self._lstm_check()
            triggered += lstm_types

            # Always write live price feed (dashboard needs it even without anomalies)
            self._write_price_feed(ts_str, price, if_score, triggered)

            if triggered:
                self._log(ts_str, price, pct_change, abs_change,
                          triggered, if_score, lstm_score)

        if self.total_events % 50 == 0 and self.total_events > 0:
            elapsed   = time.time() - self.start_time
            if_status = "ready ✓" if self.if_model else f"warming {len(self.if_buf)}/{self.IF_WARMUP}"
            print(
                f"  [stats] events={self.total_events:,} | anomalies={self.total_anomalies:,} | "
                f"{self.total_events / elapsed:.1f} ev/s | IF={if_status} | "
                f"LSTM={'on ✓' if self.lstm_model else 'off'}"
            )

    # ── Kafka topic wait ─────────────────────────────────────────────

    def _wait_for_topic(self, timeout=300):
        from kafka import KafkaAdminClient
        print(f"  Waiting for Kafka broker @ {KAFKA_BROKER}…")
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                admin  = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER,
                                          request_timeout_ms=5000)
                topics = admin.list_topics()
                admin.close()
                if KAFKA_TOPIC in topics:
                    print(f"  Topic '{KAFKA_TOPIC}' is ready ✓")
                    return
                else:
                    print(f"  Broker reachable, waiting for topic '{KAFKA_TOPIC}'…")
            except Exception as e:
                print(f"  Broker not ready yet ({e}) — retrying…")
            time.sleep(5)
        raise RuntimeError(f"Topic '{KAFKA_TOPIC}' not available after {timeout}s")

    # ── Start / resilient loop ───────────────────────────────────────

    def start(self):
        self._wait_for_topic()

        while True:
            try:
                print("  Starting Spark stream…")
                stream = (
                    self.spark.readStream.format("kafka")
                    .option("kafka.bootstrap.servers", KAFKA_BROKER)
                    .option("subscribe", KAFKA_TOPIC)
                    .option("startingOffsets", "latest")
                    .option("failOnDataLoss", "false")
                    .option("kafka.session.timeout.ms", "30000")
                    .option("kafka.request.timeout.ms", "40000")
                    .load()
                )
                query = (
                    stream.writeStream
                    .foreachBatch(self.process)
                    .trigger(processingTime="3 seconds")
                    .option("checkpointLocation", "/tmp/spark-checkpoint")
                    .start()
                )
                print("  Stream running ✓  (waiting for events…)")
                query.awaitTermination()

            except KeyboardInterrupt:
                print("\n  Shutting down gracefully…")
                break
            except Exception as e:
                print(f"\n  [WARN] Stream error: {e}")
                print("  Restarting stream in 10s…")
                time.sleep(10)
                # Clear the stale checkpoint so Spark starts fresh
                import shutil
                try:
                    shutil.rmtree("/tmp/spark-checkpoint", ignore_errors=True)
                except Exception:
                    pass


if __name__ == "__main__":
    time.sleep(5)   # give Kafka a moment after healthy check
    AnomalyDetector().start()
