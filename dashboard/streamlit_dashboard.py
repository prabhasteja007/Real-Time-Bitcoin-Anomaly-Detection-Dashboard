import os
import json
from collections import Counter

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh


# ---------- Config ----------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ANOMALY_FILE = os.path.join(BASE_DIR, "anomalies.json")


# ---------- Data loading ----------
def load_anomalies(max_rows=2000):
    """
    Load up to the last `max_rows` anomalies from anomalies.json (JSONL).
    Adds:
      - ts_dt (datetime)
      - primary_type
      - severity
    """
    if not os.path.exists(ANOMALY_FILE):
        return pd.DataFrame()

    with open(ANOMALY_FILE, "r") as f:
        lines = f.readlines()

    if not lines:
        return pd.DataFrame()

    lines = lines[-max_rows:]
    records = []

    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
            records.append(rec)
        except json.JSONDecodeError:
            continue

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # Timestamp → datetime
    df["ts_dt"] = pd.to_datetime(df.get("timestamp"), errors="coerce")

    # Normalize anomaly type list
    if "types" not in df.columns:
        df["types"] = [[] for _ in range(len(df))]
    df["types"] = df["types"].apply(lambda x: x if isinstance(x, list) else [])
    df["primary_type"] = df["types"].apply(lambda ts: ts[0] if ts else None)

    # Severity classification
    def classify_severity(types):
        tset = set(types or [])
        if "pct_spike" in tset or "abs_jump" in tset:
            return "HIGH"
        if "vol_breakout" in tset:
            return "MED"
        if "mean_reversion_break" in tset:
            return "LOW"
        return "LOW"

    df["severity"] = df["types"].apply(classify_severity)

    df = df.sort_values("ts_dt")
    return df


# ---------- Streamlit App ----------
st.set_page_config(
    page_title="BTC Anomaly Dashboard",
    layout="wide",
    page_icon="📈",
)

st.title("📈 Real-Time Bitcoin Anomaly Detection Dashboard")
st.caption("Kafka → Spark Structured Streaming → JSON → Streamlit")

# Auto-refresh every 3 seconds
st_autorefresh(interval=3000, key="refresh_counter")

df = load_anomalies(max_rows=2000)

if df.empty:
    st.warning(
        "No anomalies yet.\n\n"
        "Make sure Kafka, producer.py, and consumer.py are running so "
        "`anomalies.json` is being written."
    )
    st.stop()


# ---------- Sidebar: Filters & Help ----------
st.sidebar.header("Controls")

types_all = ["pct_spike", "abs_jump", "vol_breakout", "mean_reversion_break"]
selected_types = st.sidebar.multiselect(
    "Anomaly types",
    options=types_all,
    default=types_all,
)

severity_all = ["HIGH", "MED", "LOW"]
selected_severity = st.sidebar.multiselect(
    "Severity levels",
    options=severity_all,
    default=severity_all,
)

max_rows_view = st.sidebar.slider(
    "Show last N anomalies",
    min_value=20,
    max_value=500,
    value=100,
    step=20,
)

with st.sidebar.expander("ℹ️ What do these anomalies mean?", expanded=False):
    st.markdown(
        """
**pct_spike** – sudden % move in price over a short time window.  
**abs_jump** – large absolute $ move (e.g., ≥ $30 in ≤ 10 seconds).  
**vol_breakout** – move larger than normal volatility (e.g., ≥ 3 × σ of returns).  
**mean_reversion_break** – price stays unusually far from its recent mean.

**Severity:**
- **HIGH** → pct_spike or abs_jump  
- **MED** → vol_breakout  
- **LOW** → only mean_reversion_break
        """
    )


# ---------- Apply Filters ----------
df_view = df.copy()
df_view = df_view[df_view["severity"].isin(selected_severity)]
df_view = df_view[df_view["types"].apply(lambda lst: any(t in selected_types for t in lst))]
df_view = df_view.tail(max_rows_view)

if df_view.empty:
    st.error("No anomalies match the current filter selection.")
    st.stop()


# ---------- High Severity Banner ----------
high_df = df_view[df_view["severity"] == "HIGH"]
if not high_df.empty:
    last_high = high_df.iloc[-1]
    ts_str = last_high["ts_dt"].strftime("%Y-%m-%d %H:%M:%S") if pd.notna(last_high["ts_dt"]) else str(
        last_high.get("timestamp", "")
    )
    st.error(
        f"🚨 **Latest HIGH severity anomaly**\n\n"
        f"- Time: **{ts_str}**\n"
        f"- Price: **${last_high['price']:,.2f}**\n"
        f"- Δ%: **{last_high['pct_change']:+.4f}%**\n"
        f"- Δ$: **{last_high['abs_change']:+.2f}**\n"
        f"- Types: **{', '.join(last_high['types'])}**"
    )
else:
    st.info("✅ No HIGH severity anomalies in the current filtered window.")


# ---------- KPI Row ----------
latest = df.iloc[-1]
latest_price = latest.get("price", 0.0)
latest_pct = latest.get("pct_change", 0.0)
latest_abs = latest.get("abs_change", 0.0)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Last price", f"${latest_price:,.2f}")
col2.metric("Last Δ%", f"{latest_pct:+.4f}%", delta=f"{latest_pct:+.4f} pp")
col3.metric("Last Δ$", f"{latest_abs:+.2f}", delta=f"{latest_abs:+.2f}")
col4.metric("Total anomalies loaded", len(df))


# ---------- Tabs ----------
tab_overview, tab_timeseries, tab_table = st.tabs(
    ["📊 Overview", "📉 Time Series", "📋 Table & Export"]
)

# === Overview Tab ===
with tab_overview:
    st.subheader("Anomaly distribution (current filters)")

    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown("**By anomaly type**")
        counts = Counter()
        for tlist in df_view["types"]:
            for t in tlist:
                counts[t] += 1

        if counts:
            bar_df = pd.DataFrame(
                {"type": list(counts.keys()), "count": list(counts.values())}
            ).set_index("type")
            st.bar_chart(bar_df)
        else:
            st.info("No anomalies for selected types.")

    with col_right:
        st.markdown("**By severity**")
        sev_counts = df_view["severity"].value_counts().sort_index()
        sev_df = pd.DataFrame({"severity": sev_counts.index, "count": sev_counts.values}).set_index(
            "severity"
        )
        st.bar_chart(sev_df)

    st.markdown("---")
    st.markdown(
        """
**How to use this page**

- Use the sidebar filters to focus on specific anomaly types and severity levels.  
- The charts above quickly show whether the market is quiet (few anomalies) or in a volatile regime (many MED/HIGH anomalies).
        """
    )

# === Time Series Tab ===
with tab_timeseries:
    st.subheader("Price and anomaly rate")

    ts_col1, ts_col2 = st.columns(2)

    with ts_col1:
        st.markdown("**Price over time (filtered window)**")
        df_price = df_view.dropna(subset=["ts_dt", "price"]).set_index("ts_dt")
        if not df_price.empty:
            st.line_chart(df_price["price"])
        else:
            st.info("No price data available.")

    with ts_col2:
        st.markdown("**Anomaly rate (per minute)**")
        df_rate = df_view.dropna(subset=["ts_dt"]).copy()
        if not df_rate.empty:
            df_rate = df_rate.set_index("ts_dt")
            df_rate["count"] = 1
            try:
                rate_series = df_rate["count"].resample("1min").sum()
                if not rate_series.empty:
                    st.line_chart(rate_series)
                else:
                    st.info("Not enough data to compute anomaly rate.")
            except Exception:
                st.info("Unable to resample anomaly counts (time index issue).")
        else:
            st.info("No time information available for current anomalies.")

    st.markdown("---")
    st.markdown(
        """
The **price chart** provides short-term context.  
The **anomaly rate chart** shows periods where the detector is very active (potential stress/volatility) versus calm periods.
        """
    )

# === Table & Export Tab ===
with tab_table:
    st.subheader("Recent anomalies (filtered view)")

    table_df = df_view[
        ["ts_dt", "price", "pct_change", "abs_change", "severity", "primary_type", "types"]
    ].copy()

    table_df.rename(
        columns={
            "ts_dt": "time",
            "price": "price ($)",
            "pct_change": "Δ% (%)",
            "abs_change": "Δ$",
            "primary_type": "main_type",
        },
        inplace=True,
    )

    # Pretty time format
    table_df["time"] = table_df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")

    st.dataframe(table_df, use_container_width=True, height=420)

    csv_data = table_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="💾 Download current anomalies as CSV",
        data=csv_data,
        file_name="btc_anomalies_filtered.csv",
        mime="text/csv",
    )

    st.markdown(
        """
Use this table to drill down into individual anomalies.  
You can export the current view to CSV for further analysis in Jupyter, Excel, or other tools.
        """
    )
