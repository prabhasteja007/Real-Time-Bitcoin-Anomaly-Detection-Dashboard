"""
train_model.py — Offline training for Isolation Forest + LSTM Autoencoder.

Run this ONCE before starting the consumer pipeline:
    python collect_data.py        # fetch ~3000 historical bars
    python train_model.py         # train and save model artifacts

Artifacts saved to consumer/ directory:
  iforest_model.save    — Isolation Forest model
  iforest_scaler.save   — StandardScaler for IF features
  lstm_model.h5         — LSTM Autoencoder model
  lstm_scaler.save      — StandardScaler for LSTM sequences
  lstm_threshold.npy    — Reconstruction error threshold (float)
"""

import os
import sys
import numpy as np
import joblib
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
PRICES_FILE = os.path.join(BASE_DIR, "prices.npy")

IF_MODEL_FILE    = os.path.join(BASE_DIR, "iforest_model.save")
IF_SCALER_FILE   = os.path.join(BASE_DIR, "iforest_scaler.save")
LSTM_MODEL_FILE  = os.path.join(BASE_DIR, "lstm_model.h5")
LSTM_SCALER_FILE = os.path.join(BASE_DIR, "lstm_scaler.save")
LSTM_THRESH_FILE = os.path.join(BASE_DIR, "lstm_threshold.npy")

SEQ_LEN      = 60    # LSTM sequence length (60 one-minute bars = 1 hour)
IF_WIN       = 20    # rolling window for IF feature calculation


# ─────────────────────────────────────────────────────────────────
# Feature engineering for Isolation Forest
# ─────────────────────────────────────────────────────────────────

def make_if_features(prices: np.ndarray, window: int = IF_WIN) -> np.ndarray:
    """
    For each tick i >= window, build a 5-dimensional feature vector:
      [price, 1-tick return, 5-tick return, rolling_std_20, z_score_20]

    These capture: level, momentum, short-term volatility, and deviation.
    """
    rows = []
    for i in range(window, len(prices)):
        seg   = prices[i - window : i + 1]
        p     = prices[i]
        r1    = (prices[i] - prices[i - 1]) / (prices[i - 1] or 1e-8)
        r5    = (prices[i] - prices[i - 5]) / (prices[i - 5] or 1e-8) if i >= 5 else 0.0
        mu    = seg.mean()
        sigma = seg.std() or 1e-8
        z     = (p - mu) / sigma
        rows.append([p, r1, r5, sigma, z])
    return np.array(rows)


# ─────────────────────────────────────────────────────────────────
# Sequence builder for LSTM
# ─────────────────────────────────────────────────────────────────

def make_sequences(prices: np.ndarray, seq_len: int = SEQ_LEN) -> np.ndarray:
    """Sliding-window sequences for LSTM autoencoder training."""
    return np.array([prices[i : i + seq_len] for i in range(len(prices) - seq_len)])


# ─────────────────────────────────────────────────────────────────
# Train Isolation Forest
# ─────────────────────────────────────────────────────────────────

def train_isolation_forest(prices: np.ndarray):
    print("\n── Isolation Forest ─────────────────────────────────")
    print("  Building features…")
    X = make_if_features(prices)

    scaler  = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    print(f"  Training on {len(X_scaled):,} samples…")
    model = IsolationForest(
        n_estimators=100,
        contamination=0.05,   # expect ~5% anomaly rate
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_scaled)

    joblib.dump(model,  IF_MODEL_FILE)
    joblib.dump(scaler, IF_SCALER_FILE)
    print(f"  ✓ Model  → {IF_MODEL_FILE}")
    print(f"  ✓ Scaler → {IF_SCALER_FILE}")


# ─────────────────────────────────────────────────────────────────
# Train LSTM Autoencoder
# ─────────────────────────────────────────────────────────────────

def train_lstm(prices: np.ndarray):
    try:
        from tensorflow.keras.models import Model
        from tensorflow.keras.layers import (
            Input, LSTM, RepeatVector, TimeDistributed, Dense, Dropout
        )
        from tensorflow.keras.callbacks import EarlyStopping
    except ImportError:
        print("\n  [LSTM] TensorFlow not installed — skipping.")
        print("         pip install tensorflow")
        return

    print("\n── LSTM Autoencoder ─────────────────────────────────")
    seqs = make_sequences(prices, SEQ_LEN)
    print(f"  Sequences: {len(seqs):,} × {SEQ_LEN} steps")

    # Normalize
    scaler   = StandardScaler()
    flat     = seqs.reshape(-1, 1)
    flat_s   = scaler.fit_transform(flat)
    X        = flat_s.reshape(len(seqs), SEQ_LEN, 1)

    # Architecture: encoder → bottleneck → decoder
    inp  = Input(shape=(SEQ_LEN, 1))
    enc  = LSTM(32, activation="tanh")(inp)
    enc  = Dropout(0.2)(enc)
    rep  = RepeatVector(SEQ_LEN)(enc)
    dec  = LSTM(32, activation="tanh", return_sequences=True)(rep)
    dec  = Dropout(0.2)(dec)
    out  = TimeDistributed(Dense(1))(dec)

    model = Model(inp, out)
    model.compile(optimizer="adam", loss="mse")
    model.summary()

    early_stop = EarlyStopping(monitor="val_loss", patience=3, restore_best_weights=True)
    model.fit(
        X, X,
        epochs=20,
        batch_size=32,
        validation_split=0.1,
        callbacks=[early_stop],
        verbose=1,
    )

    # Threshold: mean + 3×std of reconstruction errors on training set
    recon  = model.predict(X, verbose=0)
    errors = np.mean((X - recon) ** 2, axis=(1, 2))
    thresh = float(np.mean(errors) + 3 * np.std(errors))

    model.save(LSTM_MODEL_FILE)
    joblib.dump(scaler, LSTM_SCALER_FILE)
    np.save(LSTM_THRESH_FILE, thresh)

    print(f"  Threshold: {thresh:.8f}")
    print(f"  ✓ Model  → {LSTM_MODEL_FILE}")
    print(f"  ✓ Scaler → {LSTM_SCALER_FILE}")
    print(f"  ✓ Thresh → {LSTM_THRESH_FILE}")


# ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not os.path.exists(PRICES_FILE):
        print(f"ERROR: {PRICES_FILE} not found.")
        print("       Run collect_data.py first.")
        sys.exit(1)

    prices = np.load(PRICES_FILE)
    print(f"Loaded {len(prices):,} prices.")
    print(f"  Range: ${prices.min():,.2f} – ${prices.max():,.2f}  |  Mean: ${prices.mean():,.2f}")

    train_isolation_forest(prices)
    train_lstm(prices)

    print("\n✅ Training complete. You can now start the consumer pipeline.")
