# 📊 Real-Time Bitcoin Anomaly Detection Dashboard

A live anomaly detection pipeline built for the course **Platforms for Big Data Processing**.  
This system processes Bitcoin price data in real time using Kafka, Spark Structured Streaming, and visualizes detected anomalies in a responsive Streamlit dashboard.

![Dashboard Snapshot](./BTC%20Anomaly%20Dashboard_page-0001.jpg)

---

## ⚙️ Architecture

```plaintext
Coinbase WebSocket → Kafka → Spark Streaming (PySpark) → JSON file → Streamlit Dashboard
```

- **Kafka**: Message broker for price ticks  
- **Spark**: Detects 4 types of real-time price anomalies  
- **Streamlit**: Displays anomalies with filtering, charts, and live updates  
![Data Stack Diagram](./data%20stack%20diagram.png)
---

## 🚨 Detected Anomalies

| Type                  | Trigger Condition                                      | Severity |
|-----------------------|--------------------------------------------------------|----------|
| `pct_spike`           | Price changes ≥ ±0.25% within 10 seconds              | High     |
| `abs_jump`            | Absolute price jumps ≥ $30 within 10 seconds          | High     |
| `vol_breakout`        | Price move ≥ 3×σ of last 60 returns (min $15)         | Medium   |
| `mean_reversion_break`| Deviation ≥ 2×σ from rolling mean (min $25)           | Low      |

These thresholds can be tuned for different volatility regimes or to explore anomaly sensitivity.

---

## 🖥 Live Dashboard Features

- 📈 Price trend line chart  
- 🚨 Real-time anomaly log with severity labels  
- 🎚 Filters: by anomaly type, severity, and recency  
- 📊 Distribution plots (anomaly type and severity breakdown)  
- 🔁 Auto-refresh every 3 seconds using `streamlit_autorefresh`

---

## 📂 Folder Structure

```
603-Project/
├── producer/            # Coinbase → Kafka
│   └── producer.py
├── consumer/            # Kafka → Spark anomaly detection
│   └── consumer.py
├── dashboard/           # Terminal and Streamlit dashboards
│   ├── dashboard.py
│   └── streamlit_dashboard.py
├── docker-compose.yml   # (optional setup)
└── README.md
```

---

## 🛠️ Getting Started

### ✅ Prerequisites

- Python 3.9+ with `venv`
- Kafka running on localhost:9092
- Java 17+ (for Spark)
- Python packages: `pyspark`, `streamlit`, `kafka-python`, `websocket-client`, `streamlit-autorefresh`

### 📦 Install Dependencies

```bash
cd 603-Project
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt  # or install manually
```

### 🧪 Run the Full Pipeline

**Terminal 1** – Start Kafka (assumes you have zookeeper/kafka setup)

```bash
docker-compose up  # if using Docker
```

**Terminal 2** – Start Coinbase producer

```bash
cd producer
python producer.py
```

**Terminal 3** – Start Spark consumer (anomaly detection)

```bash
cd consumer
python consumer.py
```

**Terminal 4** – Start Streamlit dashboard

```bash
cd dashboard
streamlit run streamlit_dashboard.py
```

---

## 🧠 Improvements & Future Work

- Add moving average overlays / Bollinger Bands  
- Alerting integration (email/Slack when high-severity anomalies occur)  
- Historical backtesting using recorded data  
- Support for multiple cryptocurrencies  
- Database integration (e.g. PostgreSQL or MongoDB)

---

## 🏁 Project Goals

✅ Real-time ingestion and streaming  
✅ Complex event processing (multi-rule anomaly detection)  
✅ Live user-facing dashboard  
✅ Modular, reproducible components  

---

## 📚 Course

**Platforms for Big Data Processing**  
Instructor: *(Add name here)*  
Project by: *(Add your name + ID if needed)*

---

## 📄 License

MIT License
