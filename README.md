# 📈 Real-Time Bitcoin Anomaly Detection Dashboard

> Stream live BTC/USD trades → detect anomalies with hybrid ML → visualize in real-time.  
> A production-style data engineering pipeline built end-to-end with Docker.

![Python](https://img.shields.io/badge/Python-3.10-3776AB?logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5-231F20?logo=apachekafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-E25A1C?logo=apachespark&logoColor=white)
![TensorFlow](https://img.shields.io/badge/TensorFlow-2.15-FF6F00?logo=tensorflow&logoColor=white)
![scikit-learn](https://img.shields.io/badge/scikit--learn-1.4-F7931E?logo=scikitlearn&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-1.32-FF4B4B?logo=streamlit&logoColor=white)
![Docker](https://img.shields.io/badge/Docker%20Compose-ready-2496ED?logo=docker&logoColor=white)

---

![Dashboard Demo](docs/dashboard.gif)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                            Docker Compose                               │
│                                                                         │
│  ┌──────────────┐   ┌─────────────────┐   ┌───────────────────────┐   │
│  │    Kraken    │   │   Apache Kafka  │   │   Spark Structured    │   │
│  │  WebSocket   │──▶│   + Zookeeper   │──▶│   Streaming Consumer  │   │
│  │  XBT/USD     │   │   (broker)      │   │                       │   │
│  └──────────────┘   └─────────────────┘   │  ┌─────────────────┐  │   │
│    live trades                             │  │  1. Rule-based  │  │   │
│    ~0.6 ticks/sec                          │  │  2. Isolation   │  │   │
│                                            │  │     Forest (ML) │  │   │
│                                            │  │  3. LSTM (DL)   │  │   │
│                                            │  └─────────────────┘  │   │
│                                            └──────────┬────────────┘   │
│                                                       │  /shared/       │
│                                                       │  anomalies.json │
│                                                       │  prices_live.json│
│                                            ┌──────────▼────────────┐   │
│                                            │  Streamlit Dashboard   │   │
│                                            │  localhost:8501        │   │
│                                            │  auto-refresh 3s       │   │
│                                            └───────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Live Performance (measured on real data)

| Metric | Value |
|---|---|
| Trade throughput | ~0.6 ticks/sec (Kraken XBT/USD) |
| End-to-end latency | < 6 seconds (WebSocket → dashboard) |
| Spark batch trigger | 3 seconds |
| IF model retraining | Every 500 ticks (online learning) |
| LSTM max anomaly score | 2.28× detection threshold |
| Max price jump detected | $42.70 in a single tick |
| Anomaly detection layers | 3 (rule + ML + deep learning) |

---

## Detection Layers

| Layer | Detector | Trigger | Severity |
|---|---|---|---|
| Rule | `pct_spike` | \|Δ%\| ≥ 0.15% between consecutive trades | 🔴 HIGH |
| Rule | `abs_jump` | \|Δ$\| ≥ $20 between consecutive trades | 🔴 HIGH |
| Rule | `vol_breakout` | \|Δ$\| ≥ 2.5× rolling σ of returns | 🟡 MED |
| Rule | `mean_reversion_break` | Price deviates ≥ 2σ from rolling mean | 🟢 LOW |
| ML | `iforest_anomaly` | Isolation Forest outlier score | 🟡 MED |
| Deep ML | `lstm_anomaly` | LSTM autoencoder reconstruction error > dynamic threshold | 🔴 HIGH |

**Why 3 layers?**
- Rules catch sharp spikes instantly with zero warm-up
- Isolation Forest catches subtle structural anomalies rules miss — retrains online every 500 ticks to adapt to market regime changes
- LSTM Autoencoder detects anomalies in the *shape* of 60-tick price sequences — gradual manipulation and complex patterns invisible to single-tick rules

---

## Tech Stack — Design Decisions

| Component | Technology | Why this choice |
|---|---|---|
| **Ingestion** | Kraken WebSocket | Free, no API key, geo-accessible in US. Binance is blocked (HTTP 451) |
| **Message broker** | Apache Kafka (Confluent) | Decouples producer from consumer; enables replay; fault-tolerant |
| **Stream processing** | Spark 3.5 Structured Streaming | Scales horizontally; same code runs on laptop or 100-node cluster |
| **Outlier detection** | Isolation Forest (scikit-learn) | Unsupervised — no labeled data needed; supports online retraining |
| **Sequence modeling** | LSTM Autoencoder (TensorFlow) | Captures temporal dependencies across 60-tick windows |
| **Dashboard** | Streamlit | Python-native; auto-refresh; zero frontend code |
| **Orchestration** | Docker Compose | One-command reproducible startup; named volumes for service isolation |

---

## Quickstart

### Prerequisites
- Docker Desktop (with Compose) — 4 GB RAM free
- Python 3.10+ (for offline training only)

### 1. Clone and start the pipeline

```bash
git clone https://github.com/prabhasteja007/Real-Time-Bitcoin-Anomaly-Detection-Dashboard
cd Real-Time-Bitcoin-Anomaly-Detection-Dashboard
docker compose up --build
```

Open **http://localhost:8501**

> ⏳ First run: Spark downloads the Kafka connector JAR (~2 minutes).  
> Watch the consumer logs for: `Stream running ✓`

### 2. Enable LSTM (optional — recommended)

Run once locally to train the LSTM on historical Kraken data:

```bash
cd consumer
pip install -r requirements.txt
python collect_data.py --bars 5000    # ~3 days of 1-min BTC bars from Kraken REST API
python train_model.py                  # trains Isolation Forest + LSTM autoencoder
```

Then rebuild to bake the models into the image:
```bash
docker compose up --build consumer
```

---

## Project Structure

```
├── docker-compose.yml              # 5-service orchestration
├── producer/
│   ├── producer.py                 # Kraken WebSocket → Kafka
│   └── Dockerfile
├── consumer/
│   ├── consumer.py                 # Spark Streaming + 3-layer anomaly detection
│   ├── collect_data.py             # Fetch historical BTC data (Kraken REST API)
│   ├── train_model.py              # Train Isolation Forest + LSTM offline
│   ├── preload_jars.py             # Pre-download Spark-Kafka JAR at build time
│   ├── requirements.txt
│   └── Dockerfile
└── dashboard/
    ├── streamlit_dashboard.py      # Live price chart + anomaly visualization
    ├── requirements.txt
    └── Dockerfile
```

---

## Dashboard Features

- **Live BTC/USD price chart** with anomaly dots colored by severity (red/orange/yellow)
- **Real-time KPIs** — current price, session uptime, ticks processed, anomaly rate
- **HIGH severity alert banner** — appears immediately when a sharp move is detected
- **IF warmup progress bar** — shows model training status in real-time
- **Overview tab** — anomaly type distribution and severity breakdown (color-coded bar charts)
- **Time Series tab** — price at anomaly events, frequency area chart, Δ% histogram
- **ML Scores tab** — Isolation Forest score scatter and LSTM reconstruction error over time
- **Event log** — filterable table, newest-first, CSV export
- **Sidebar quick filters** — preset buttons for HIGH-only or ML-only views
- **Mobile-friendly** — responsive layout, sidebar collapsed by default

---

## Real-World Use Cases

| Who | How |
|---|---|
| **Algorithmic traders** | Anomaly events as entry/exit signals for automated bots |
| **Crypto exchanges** | Detect wash trading, spoofing, and market manipulation |
| **Hedge fund risk desks** | Real-time crypto exposure monitoring 24/7 |
| **Retail investors** | Alerts for unusual moves while sleeping (crypto never stops) |
| **Researchers** | Study whether detected anomalies predict price direction |

---

## Free Deployment (Oracle Cloud Free Tier)

2 AMD OCPUs + 1 GB RAM — always free, no credit card required after signup.

```bash
# On the Oracle VM:
sudo apt install docker.io docker-compose-plugin -y
git clone https://github.com/prabhasteja007/Real-Time-Bitcoin-Anomaly-Detection-Dashboard
cd Real-Time-Bitcoin-Anomaly-Detection-Dashboard
docker compose up -d
```

Open port 8501 in Oracle's security list → access via public IP.

---

## Future Improvements

- Add CCXT for multi-exchange ingestion (Binance, Coinbase, Bybit)
- Persist anomaly history to PostgreSQL / InfluxDB for long-term analysis
- Slack / Telegram webhook alerts on HIGH severity events
- Retrain LSTM on tick-level data (currently trained on 1-min bars)
- Kubernetes deployment with Helm chart for horizontal scaling

---

## Course

**Platforms for Big Data Processing (DATA 603)**  
*Real-time streaming pipeline with Kafka, Spark, and ML-based anomaly detection*
