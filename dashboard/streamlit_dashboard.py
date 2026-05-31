"""
streamlit_dashboard.py — Real-Time BTC Anomaly Detection Dashboard
Production-ready, mobile-friendly, deployment-grade UI.

Data sources (shared Docker volume /shared/):
  prices_live.json  — live tick feed from Spark consumer (every 3s)
  anomalies.json    — JSONL anomaly event log from Spark consumer
"""

import os
import json
from collections import Counter

import altair as alt
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# ─────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────
SHARED_DIR   = os.getenv("SHARED_DIR",   "/shared")
ANOMALY_FILE = os.getenv("ANOMALY_FILE", os.path.join(SHARED_DIR, "anomalies.json"))
PRICE_FILE   = os.path.join(SHARED_DIR, "prices_live.json")

ALL_TYPES = [
    "pct_spike", "abs_jump", "vol_breakout",
    "mean_reversion_break", "iforest_anomaly", "lstm_anomaly",
]

TYPE_META = {
    "pct_spike":            ("Rule",    "HIGH", "Price moved ≥ 0.15% in one tick"),
    "abs_jump":             ("Rule",    "HIGH", "Price moved ≥ $20 in one tick"),
    "vol_breakout":         ("Rule",    "MED",  "Move ≥ 2.5× usual volatility"),
    "mean_reversion_break": ("Rule",    "LOW",  "Price drifted ≥ 2σ from rolling mean"),
    "iforest_anomaly":      ("ML",      "MED",  "Isolation Forest flagged unusual pattern"),
    "lstm_anomaly":         ("Deep ML", "HIGH", "LSTM sequence model: high reconstruction error"),
}

SEV_COLOR  = {"HIGH": "#ff4b4b", "MED": "#ffa500", "LOW": "#f5c518"}
SEV_BG     = {"HIGH": "#3d0000", "MED": "#3d1f00", "LOW": "#2d2800"}
SEV_DOMAIN = ["HIGH", "MED", "LOW"]
SEV_RANGE  = ["#ff4b4b", "#ffa500", "#f5c518"]

# ─────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="BTC Anomaly Monitor",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",   # collapsed by default → cleaner on mobile
)

st_autorefresh(interval=3000, key="ar")

# ─────────────────────────────────────────────────────────────────
# GLOBAL CSS — professional dark theme, mobile-friendly
# ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* ── Base ── */
html, body, [class*="css"] { font-family: 'Inter', 'Segoe UI', sans-serif; }

/* ── Metric cards ── */
[data-testid="metric-container"] {
    background: #161b27;
    border: 1px solid #2a3044;
    border-radius: 10px;
    padding: 14px 18px;
}
[data-testid="stMetricLabel"]  { font-size: 0.72rem; color: #8892a4; text-transform: uppercase; letter-spacing: 0.06em; }
[data-testid="stMetricValue"]  { font-size: 1.55rem; font-weight: 700; color: #e8eaf0; }
[data-testid="stMetricDelta"]  { font-size: 0.82rem; }

/* ── Tabs ── */
[data-testid="stTabs"] button { font-size: 0.85rem; font-weight: 600; padding: 6px 16px; }

/* ── Alert banner override ── */
[data-testid="stAlert"] { border-radius: 8px; }

/* ── Sidebar ── */
[data-testid="stSidebar"] { background: #0d1117; border-right: 1px solid #21262d; }

/* ── Dataframe ── */
[data-testid="stDataFrame"] { border-radius: 8px; overflow: hidden; }

/* ── Divider ── */
hr { border-color: #21262d !important; margin: 1.2rem 0 !important; }

/* ── Section headers ── */
h2, h3 { color: #e8eaf0 !important; letter-spacing: -0.02em; }

/* ── Caption ── */
small, .caption, [data-testid="stCaptionContainer"] { color: #6b7894 !important; }

/* ── Pill badge ── */
.pill {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 20px;
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.04em;
    margin-right: 4px;
}
.pill-high { background:#3d0000; color:#ff4b4b; border:1px solid #ff4b4b40; }
.pill-med  { background:#3d1f00; color:#ffa500; border:1px solid #ffa50040; }
.pill-low  { background:#2d2800; color:#f5c518; border:1px solid #f5c51840; }
.pill-rule { background:#1a2035; color:#7c9cbf; border:1px solid #7c9cbf40; }
.pill-ml   { background:#1a2835; color:#00d4aa; border:1px solid #00d4aa40; }

/* ── Info box ── */
.info-box {
    background: #161b27;
    border: 1px solid #2a3044;
    border-left: 3px solid #00b4d8;
    border-radius: 6px;
    padding: 10px 14px;
    font-size: 0.83rem;
    color: #8892a4;
    line-height: 1.6;
}

/* ── Mobile: stack columns ── */
@media (max-width: 768px) {
    [data-testid="column"] { min-width: 100% !important; }
    [data-testid="stMetricValue"] { font-size: 1.2rem; }
}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────
# DATA LOADERS
# ─────────────────────────────────────────────────────────────────

def load_prices() -> pd.DataFrame:
    if not os.path.exists(PRICE_FILE):
        return pd.DataFrame()
    try:
        with open(PRICE_FILE, "r") as f:
            data = json.load(f)
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df["ts_dt"]   = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
        df["price"]   = pd.to_numeric(df["price"],    errors="coerce")
        df["if_score"]= pd.to_numeric(df.get("if_score", 0), errors="coerce").fillna(0.0)
        df["if_ready"]= df.get("if_ready", False).astype(bool)
        return df.dropna(subset=["ts_dt","price"]).sort_values("ts_dt").reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


def load_anomalies(max_rows: int = 3000) -> pd.DataFrame:
    if not os.path.exists(ANOMALY_FILE):
        return pd.DataFrame()
    try:
        with open(ANOMALY_FILE, "r") as f:
            lines = [l.strip() for l in f.readlines()[-max_rows:] if l.strip()]
    except Exception:
        return pd.DataFrame()

    records = [json.loads(l) for l in lines if l]
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["ts_dt"]       = pd.to_datetime(df.get("timestamp"), errors="coerce", utc=True)
    df["types"]       = df.get("types", pd.Series([[] for _ in range(len(df))])).apply(
                            lambda x: x if isinstance(x, list) else [])
    df["primary_type"]= df["types"].apply(lambda t: t[0] if t else "unknown")
    df["if_score"]    = pd.to_numeric(df.get("if_score"),   errors="coerce").fillna(0.0)
    df["lstm_score"]  = pd.to_numeric(df.get("lstm_score"), errors="coerce").fillna(0.0)
    df["price"]       = pd.to_numeric(df.get("price"),      errors="coerce")
    df["pct_change"]  = pd.to_numeric(df.get("pct_change"), errors="coerce").fillna(0.0)
    df["abs_change"]  = pd.to_numeric(df.get("abs_change"), errors="coerce").fillna(0.0)

    if "severity" not in df.columns:
        def _sev(types):
            tset = set(types)
            if tset & {"pct_spike","abs_jump","lstm_anomaly"}: return "HIGH"
            if tset & {"vol_breakout","iforest_anomaly"}:       return "MED"
            return "LOW"
        df["severity"] = df["types"].apply(_sev)

    return df.sort_values("ts_dt").reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────
# LOAD DATA
# ─────────────────────────────────────────────────────────────────
price_df   = load_prices()
anomaly_df = load_anomalies()
pipeline_up = not price_df.empty


# ─────────────────────────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────────────────────────
st.markdown("## 📈 Real-Time Bitcoin Anomaly Monitor")
st.markdown(
    "<span style='color:#6b7894;font-size:0.82rem'>"
    "Kraken WebSocket &nbsp;→&nbsp; Apache Kafka &nbsp;→&nbsp; "
    "Spark Structured Streaming &nbsp;→&nbsp; "
    "Rule-based + Isolation Forest + LSTM &nbsp;→&nbsp; Streamlit"
    "</span>",
    unsafe_allow_html=True,
)

# ─────────────────────────────────────────────────────────────────
# PIPELINE NOT READY STATE
# ─────────────────────────────────────────────────────────────────
if not pipeline_up:
    st.divider()
    st.markdown("""
<div class="info-box">
⏳ <strong>Waiting for pipeline data…</strong><br><br>
The Spark consumer is initializing. On first run it downloads the Kafka connector JAR (~2 min).<br>
Watch the <code>consumer</code> container logs for: <code>Stream running ✓</code>
</div>
""", unsafe_allow_html=True)
    with st.expander("📖 How this works"):
        st.markdown("""
**Data flow (left → right):**
1. **Kraken WebSocket** streams every BTC/USD trade in real-time (free, no API key)
2. **Apache Kafka** buffers the trade stream — decouples producers from consumers and enables replay
3. **Apache Spark** reads Kafka in 3-second micro-batches and runs anomaly detection
4. **This dashboard** reads the output files every 3 seconds and updates automatically

**Why Kafka + Spark instead of just reading the WebSocket directly?**
Kafka makes the pipeline fault-tolerant: if the consumer crashes, it resumes from where it left off.
Spark scales horizontally — the same code runs on a laptop or a 100-node cluster.
""")
    st.stop()


# ─────────────────────────────────────────────────────────────────
# COMPUTE STATS
# ─────────────────────────────────────────────────────────────────
last_tick  = price_df.iloc[-1]
prev_tick  = price_df.iloc[-2] if len(price_df) > 1 else last_tick
cur_price  = last_tick["price"]
prev_price = prev_tick["price"]
delta_abs  = cur_price - prev_price
delta_pct  = delta_abs / prev_price * 100 if prev_price else 0
if_ready   = bool(last_tick.get("if_ready", False))

n_ticks    = len(price_df)
n_anom     = len(anomaly_df) if not anomaly_df.empty else 0
n_high     = (anomaly_df["severity"] == "HIGH").sum() if not anomaly_df.empty else 0
n_med      = (anomaly_df["severity"] == "MED").sum()  if not anomaly_df.empty else 0
anom_rate  = n_anom / max(n_ticks, 1) * 100

first_ts   = price_df["ts_dt"].iloc[0]
last_ts    = price_df["ts_dt"].iloc[-1]
elapsed_s  = (last_ts - first_ts).total_seconds()
elapsed    = f"{int(elapsed_s//3600)}h {int((elapsed_s%3600)//60)}m" \
             if elapsed_s >= 3600 else \
             f"{int(elapsed_s//60)}m {int(elapsed_s%60)}s"

prices_arr = price_df["price"].dropna()
p_high_24  = prices_arr.max()
p_low_24   = prices_arr.min()
p_range    = p_high_24 - p_low_24

# ─────────────────────────────────────────────────────────────────
# ACTIVE HIGH ALERT — top of page, most urgent
# ─────────────────────────────────────────────────────────────────
if not anomaly_df.empty:
    high_recent = anomaly_df[anomaly_df["severity"] == "HIGH"]
    if not high_recent.empty:
        lh = high_recent.iloc[-1]
        ts_s = lh["ts_dt"].strftime("%H:%M:%S UTC") if pd.notna(lh["ts_dt"]) else "—"
        detectors = " · ".join(f"`{t}`" for t in lh["types"])
        st.error(
            f"🔴 **ACTIVE ALERT** &nbsp;|&nbsp; {ts_s} &nbsp;|&nbsp; "
            f"**${lh['price']:,.2f}** &nbsp;|&nbsp; "
            f"Δ = {lh['pct_change']:+.4f}% &nbsp;|&nbsp; {detectors}",
            icon=None,
        )

st.divider()

# ─────────────────────────────────────────────────────────────────
# KPI METRICS — two rows of 3 for mobile friendliness
# ─────────────────────────────────────────────────────────────────
r1c1, r1c2, r1c3 = st.columns(3)
r1c1.metric(
    "BTC / USD",
    f"${cur_price:,.2f}",
    f"{delta_pct:+.3f}%  (${delta_abs:+.2f})",
    help="Latest trade price from Kraken WebSocket. Δ% shows change from the previous tick.",
)
r1c2.metric(
    "Session Range",
    f"${p_range:,.2f}",
    f"H: ${p_high_24:,.2f}  L: ${p_low_24:,.2f}",
    help="Highest and lowest price seen since this session started.",
)
r1c3.metric(
    "Session Uptime",
    elapsed,
    f"{n_ticks:,} ticks processed",
    help="Time since the first price tick was received in this session.",
)

st.markdown("")  # small vertical gap

r2c1, r2c2, r2c3 = st.columns(3)
r2c1.metric(
    "Anomalies Detected",
    f"{n_anom:,}",
    f"🔴 {n_high} HIGH · 🟠 {n_med} MED",
    help="Total anomaly events logged. An event can trigger multiple detectors simultaneously.",
)
r2c2.metric(
    "Anomaly Rate",
    f"{anom_rate:.1f}%",
    "normal: 5–20%" if 5 <= anom_rate <= 20 else ("↑ high" if anom_rate > 20 else "↓ quiet"),
    help="Percentage of price ticks that triggered at least one anomaly detector. "
         "5–20% is typical for active crypto markets.",
)
r2c3.metric(
    "Isolation Forest",
    "Ready ✓" if if_ready else "Warming up…",
    f"ML model active" if if_ready else f"{min(n_ticks,300)}/300 ticks",
    help="The Isolation Forest ML model needs 300 ticks to train before it activates. "
         "Rule-based detectors fire immediately from tick 1.",
)

# IF warmup progress bar
if not if_ready:
    warmup_pct = min(n_ticks / 300, 1.0)
    st.markdown(
        f"<div style='font-size:0.75rem;color:#6b7894;margin:-8px 0 4px'>IF warm-up: {n_ticks}/300 ticks</div>",
        unsafe_allow_html=True,
    )
    st.progress(warmup_pct)

st.divider()

# ─────────────────────────────────────────────────────────────────
# LIVE PRICE CHART
# ─────────────────────────────────────────────────────────────────
st.markdown("### Live BTC/USD Price Feed")
st.markdown(
    "<span class='info-box' style='display:inline-block;padding:4px 12px;margin-bottom:8px'>"
    "🔵 Price line &nbsp;&nbsp; 🔴 HIGH anomaly &nbsp;&nbsp; "
    "🟠 MED anomaly &nbsp;&nbsp; 🟡 LOW anomaly &nbsp;&nbsp; "
    "<em>Hover dots for details · Scroll to zoom · Drag to pan</em>"
    "</span>",
    unsafe_allow_html=True,
)

chart_data = price_df[["ts_dt","price"]].dropna().copy()
padding    = max((prices_arr.max() - prices_arr.min()) * 0.4, 30)

price_line = alt.Chart(chart_data).mark_line(
    color="#00b4d8", strokeWidth=1.8
).encode(
    x=alt.X("ts_dt:T", title=None,
            axis=alt.Axis(format="%H:%M:%S", labelAngle=-30, labelColor="#6b7894",
                          gridColor="#21262d", tickColor="#21262d")),
    y=alt.Y("price:Q", title="Price (USD)",
            scale=alt.Scale(zero=False,
                            domain=[prices_arr.min()-padding, prices_arr.max()+padding]),
            axis=alt.Axis(format="$,.0f", labelColor="#6b7894",
                          gridColor="#21262d", tickColor="#21262d")),
    tooltip=[
        alt.Tooltip("ts_dt:T",  title="Time",  format="%H:%M:%S"),
        alt.Tooltip("price:Q",  title="Price", format="$,.2f"),
    ],
)

layers = [price_line]

if not anomaly_df.empty:
    anom_win = anomaly_df[
        anomaly_df["ts_dt"] >= price_df["ts_dt"].iloc[0]
    ].dropna(subset=["ts_dt","price"]).copy()

    if not anom_win.empty:
        anom_dots = alt.Chart(anom_win).mark_circle(size=80, opacity=0.9).encode(
            x=alt.X("ts_dt:T"),
            y=alt.Y("price:Q",
                    scale=alt.Scale(zero=False,
                                    domain=[prices_arr.min()-padding, prices_arr.max()+padding])),
            color=alt.Color("severity:N",
                scale=alt.Scale(domain=SEV_DOMAIN, range=SEV_RANGE),
                legend=alt.Legend(title="Severity", orient="top-right",
                                  labelColor="#e8eaf0", titleColor="#8892a4",
                                  symbolSize=100),
            ),
            tooltip=[
                alt.Tooltip("ts_dt:T",       title="Time",      format="%H:%M:%S"),
                alt.Tooltip("price:Q",        title="Price",     format="$,.2f"),
                alt.Tooltip("severity:N",     title="Severity"),
                alt.Tooltip("primary_type:N", title="Detector"),
                alt.Tooltip("pct_change:Q",   title="Δ%",        format="+.4f"),
                alt.Tooltip("abs_change:Q",   title="Δ$",        format="+.2f"),
                alt.Tooltip("if_score:Q",     title="IF Score",  format=".3f"),
            ],
        )
        layers.append(anom_dots)

price_chart = (
    alt.layer(*layers)
    .properties(height=340, background="transparent")
    .configure_view(strokeOpacity=0)
    .interactive()
)
st.altair_chart(price_chart, use_container_width=True)

n_win = len(anomaly_df[anomaly_df["ts_dt"] >= price_df["ts_dt"].iloc[0]]) \
        if not anomaly_df.empty else 0
st.caption(f"Showing {n_ticks:,} ticks · {n_win} anomaly events in current window")

st.divider()

# ─────────────────────────────────────────────────────────────────
# NO ANOMALIES YET STATE
# ─────────────────────────────────────────────────────────────────
if anomaly_df.empty:
    st.markdown("""
<div class="info-box">
✅ <strong>Pipeline running — no anomalies detected yet.</strong><br><br>
BTC is currently quiet. Rule-based detectors fire on price spikes ≥ 0.15% or ≥ $20.
The Isolation Forest ML model activates after 300 ticks.
</div>
""", unsafe_allow_html=True)
    st.stop()


# ─────────────────────────────────────────────────────────────────
# SIDEBAR — filters + documentation
# ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🔧 Filters")

    # Quick preset buttons
    st.markdown("**Quick presets**")
    qc1, qc2, qc3 = st.columns(3)
    preset = None
    if qc1.button("🔴 HIGH", use_container_width=True, key="q_high"):
        preset = "high"
    if qc2.button("🤖 ML", use_container_width=True, key="q_ml"):
        preset = "ml"
    if qc3.button("All", use_container_width=True, key="q_all"):
        preset = "all"

    default_types = ALL_TYPES
    default_sev   = ["HIGH", "MED", "LOW"]
    if preset == "high":
        default_types = ["pct_spike", "abs_jump", "lstm_anomaly"]
        default_sev   = ["HIGH"]
    elif preset == "ml":
        default_types = ["iforest_anomaly", "lstm_anomaly"]
        default_sev   = ["HIGH", "MED"]

    st.markdown("")
    selected_sev = st.multiselect(
        "Severity level",
        ["HIGH", "MED", "LOW"],
        default=default_sev,
        help="HIGH = sharp price moves. MED = unusual patterns. LOW = slow drift.",
    )
    selected_types = st.multiselect(
        "Detector type",
        ALL_TYPES,
        default=default_types,
        help="Filter by which anomaly detector triggered the event.",
    )
    max_rows_view = st.slider(
        "Events to display", 20, 500, 200, step=20,
        help="Number of most recent anomaly events shown in charts and table.",
    )

    st.divider()

    st.markdown("## 📖 Detector Reference")
    for t, (layer, sev, desc) in TYPE_META.items():
        pill_sev   = f"pill-{sev.lower()}"
        pill_layer = "pill-ml" if layer in ("ML","Deep ML") else "pill-rule"
        st.markdown(
            f"<span class='pill {pill_sev}'>{sev}</span>"
            f"<span class='pill {pill_layer}'>{layer}</span>"
            f"<br><strong style='font-size:0.85rem'>{t}</strong>"
            f"<br><span style='font-size:0.75rem;color:#6b7894'>{desc}</span>"
            f"<br><br>",
            unsafe_allow_html=True,
        )

    st.divider()

    st.markdown("## 💡 Who uses this?")
    st.markdown("""
<div style='font-size:0.8rem;color:#8892a4;line-height:1.7'>
<b style='color:#e8eaf0'>Algo traders</b> — anomaly events as entry/exit signals<br>
<b style='color:#e8eaf0'>Risk desks</b> — real-time exposure monitoring 24/7<br>
<b style='color:#e8eaf0'>Exchanges</b> — detect wash trading & manipulation<br>
<b style='color:#e8eaf0'>Retail investors</b> — alerts while sleeping (crypto never stops)<br>
<b style='color:#e8eaf0'>Researchers</b> — study whether anomalies predict direction
</div>
""", unsafe_allow_html=True)

    st.divider()
    st.caption("Auto-refreshes every 3 seconds · Data: Kraken public WebSocket")


# ─────────────────────────────────────────────────────────────────
# APPLY FILTERS
# ─────────────────────────────────────────────────────────────────
if not selected_sev or not selected_types:
    st.warning("⚠️ No filters selected — adjust the sidebar filters to see anomalies.")
    st.stop()

mask = (
    anomaly_df["severity"].isin(selected_sev) &
    anomaly_df["types"].apply(lambda lst: any(t in selected_types for t in lst))
)
df_view = anomaly_df[mask].tail(max_rows_view).copy()

if df_view.empty:
    st.markdown("""
<div class="info-box">
⚠️ <strong>No events match the current filters.</strong><br>
Try adding more detector types or severity levels in the sidebar.
</div>
""", unsafe_allow_html=True)
    st.stop()

# Active filter summary
active_sev_pills = " ".join(
    f"<span class='pill pill-{s.lower()}'>{s}</span>" for s in selected_sev
)
st.markdown(
    f"<div style='margin-bottom:8px;font-size:0.8rem;color:#6b7894'>"
    f"Showing <strong style='color:#e8eaf0'>{len(df_view):,}</strong> events &nbsp;·&nbsp; "
    f"Severity: {active_sev_pills} &nbsp;·&nbsp; "
    f"{len(selected_types)}/{len(ALL_TYPES)} detector types active"
    f"</div>",
    unsafe_allow_html=True,
)


# ─────────────────────────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────────────────────────
tab_overview, tab_timeseries, tab_ml, tab_table = st.tabs([
    "📊  Overview",
    "📉  Time Series",
    "🤖  ML Scores",
    "📋  Event Log",
])


# ═══════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ═══════════════════════════════════════════════════════════════════
with tab_overview:
    # ── Summary sentence ──────────────────────────────────────────
    sev_counts = df_view["severity"].value_counts().reindex(["HIGH","MED","LOW"], fill_value=0)
    type_counts = Counter(t for lst in df_view["types"] for t in lst)
    top_detector = max(type_counts, key=type_counts.get) if type_counts else "—"

    st.markdown(
        f"<div class='info-box'>"
        f"In the last <strong>{len(df_view):,} events</strong>: &nbsp;"
        f"<span class='pill pill-high'>{sev_counts['HIGH']} HIGH</span>"
        f"<span class='pill pill-med'>{sev_counts['MED']} MED</span>"
        f"<span class='pill pill-low'>{sev_counts['LOW']} LOW</span>"
        f"&nbsp; Top detector: <strong style='color:#e8eaf0'>{top_detector}</strong>"
        f"</div>",
        unsafe_allow_html=True,
    )
    st.markdown("")

    col_l, col_r = st.columns(2)

    with col_l:
        st.markdown("#### Anomaly type counts")
        st.caption("How many times each detector fired. One event can trigger multiple detectors.")
        c_df = pd.DataFrame([
            {"type": t, "count": v, "severity": TYPE_META.get(t, ("?","LOW",""))[1]}
            for t, v in type_counts.items()
        ]).sort_values("count", ascending=False)

        bars = alt.Chart(c_df).mark_bar(
            cornerRadiusTopLeft=5, cornerRadiusTopRight=5
        ).encode(
            x=alt.X("type:N", sort="-y", title=None,
                    axis=alt.Axis(labelAngle=-35, labelColor="#8892a4", labelFontSize=11)),
            y=alt.Y("count:Q", title="Count",
                    axis=alt.Axis(labelColor="#8892a4", gridColor="#21262d")),
            color=alt.Color("severity:N",
                scale=alt.Scale(domain=SEV_DOMAIN, range=SEV_RANGE),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("type:N",     title="Detector"),
                alt.Tooltip("count:Q",    title="Count"),
                alt.Tooltip("severity:N", title="Severity"),
            ],
        ).properties(height=270, background="transparent").configure_view(strokeOpacity=0)
        st.altair_chart(bars, use_container_width=True)

    with col_r:
        st.markdown("#### Severity breakdown")
        st.caption("Distribution of events by impact level.")
        sev_df = sev_counts.reset_index()
        sev_df.columns = ["severity", "count"]
        sev_df["pct"] = (sev_df["count"] / sev_df["count"].sum() * 100).round(1)

        sev_bars = alt.Chart(sev_df).mark_bar(
            cornerRadiusTopLeft=5, cornerRadiusTopRight=5
        ).encode(
            x=alt.X("severity:N", sort=["HIGH","MED","LOW"], title=None,
                    axis=alt.Axis(labelColor="#8892a4", labelFontSize=13)),
            y=alt.Y("count:Q", title="Count",
                    axis=alt.Axis(labelColor="#8892a4", gridColor="#21262d")),
            color=alt.Color("severity:N",
                scale=alt.Scale(domain=SEV_DOMAIN, range=SEV_RANGE),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("severity:N"),
                alt.Tooltip("count:Q", title="Count"),
                alt.Tooltip("pct:Q",   title="%", format=".1f"),
            ],
        ).properties(height=220, background="transparent").configure_view(strokeOpacity=0)
        st.altair_chart(sev_bars, use_container_width=True)

        # Severity breakdown as text
        for _, row in sev_df.iterrows():
            c = SEV_COLOR.get(row["severity"], "#fff")
            bar_fill = int(row["pct"] / 2)
            bar_html  = f"<span style='color:{c}'>{'█' * bar_fill}</span>" \
                        f"<span style='color:#21262d'>{'█' * (50 - bar_fill)}</span>"
            st.markdown(
                f"<div style='font-size:0.8rem;margin:2px 0'>"
                f"<span style='color:{c};font-weight:700;width:40px;display:inline-block'>"
                f"{row['severity']}</span> "
                f"{row['count']} events ({row['pct']}%) {bar_html}"
                f"</div>",
                unsafe_allow_html=True,
            )


# ═══════════════════════════════════════════════════════════════════
# TAB 2 — TIME SERIES
# ═══════════════════════════════════════════════════════════════════
with tab_timeseries:
    col_l, col_r = st.columns(2)

    with col_l:
        st.markdown("#### Price at anomaly events")
        st.caption("BTC price plotted only at moments anomalies were detected.")
        p_df = df_view.dropna(subset=["ts_dt","price"]).copy()
        if not p_df.empty:
            p_chart = alt.Chart(p_df).mark_line(
                color="#00b4d8", strokeWidth=1.4, point=alt.OverlayMarkDef(size=25, opacity=0.6)
            ).encode(
                x=alt.X("ts_dt:T", title=None,
                        axis=alt.Axis(format="%H:%M", labelAngle=-30, labelColor="#6b7894",
                                      gridColor="#21262d")),
                y=alt.Y("price:Q", title="Price (USD)", scale=alt.Scale(zero=False),
                        axis=alt.Axis(format="$,.0f", labelColor="#6b7894", gridColor="#21262d")),
                color=alt.Color("severity:N",
                    scale=alt.Scale(domain=SEV_DOMAIN, range=SEV_RANGE), legend=None),
                tooltip=[alt.Tooltip("ts_dt:T", format="%H:%M:%S"),
                         alt.Tooltip("price:Q", title="Price", format="$,.2f"),
                         alt.Tooltip("severity:N"),
                         alt.Tooltip("primary_type:N", title="Detector")],
            ).properties(height=280, background="transparent").configure_view(strokeOpacity=0).interactive()
            st.altair_chart(p_chart, use_container_width=True)
        else:
            st.info("No price data available for current filter.")

    with col_r:
        st.markdown("#### Anomaly frequency over time")
        st.caption("Number of anomaly events per minute. Spikes indicate volatile periods.")
        rate_df = df_view.dropna(subset=["ts_dt"]).set_index("ts_dt").resample("1min").size().reset_index()
        rate_df.columns = ["minute", "count"]

        if len(rate_df) >= 2:
            area_chart = alt.Chart(rate_df).mark_area(
                color="#00b4d8", opacity=0.25,
                line={"color": "#00b4d8", "strokeWidth": 1.5},
            ).encode(
                x=alt.X("minute:T", title=None,
                        axis=alt.Axis(format="%H:%M", labelAngle=-30, labelColor="#6b7894",
                                      gridColor="#21262d")),
                y=alt.Y("count:Q", title="Events / min",
                        axis=alt.Axis(labelColor="#6b7894", gridColor="#21262d")),
                tooltip=[alt.Tooltip("minute:T", title="Time", format="%H:%M"),
                         alt.Tooltip("count:Q",  title="Events")],
            ).properties(height=280, background="transparent").configure_view(strokeOpacity=0).interactive()
            st.altair_chart(area_chart, use_container_width=True)
        else:
            st.markdown("""
<div class="info-box">Not enough data for rate chart yet — need at least 2 minutes of anomalies.</div>
""", unsafe_allow_html=True)

    # ── Δ% distribution ───────────────────────────────────────────
    st.markdown("#### Price change (Δ%) distribution at anomaly events")
    st.caption(
        "Histogram of how large the price move was when each anomaly fired. "
        "Wider spread = more volatile anomalies."
    )
    pct_df = df_view[df_view["pct_change"].abs() > 0].copy()
    if not pct_df.empty:
        hist = alt.Chart(pct_df).mark_bar(opacity=0.75).encode(
            x=alt.X("pct_change:Q", bin=alt.Bin(maxbins=40), title="Δ% per tick",
                    axis=alt.Axis(format="+.3f", labelColor="#6b7894", gridColor="#21262d")),
            y=alt.Y("count():Q", title="Event count",
                    axis=alt.Axis(labelColor="#6b7894", gridColor="#21262d")),
            color=alt.Color("severity:N",
                scale=alt.Scale(domain=SEV_DOMAIN, range=SEV_RANGE),
                legend=alt.Legend(title="Severity", labelColor="#e8eaf0", titleColor="#8892a4"),
            ),
            tooltip=[alt.Tooltip("pct_change:Q", bin=True, title="Δ% range"),
                     alt.Tooltip("count():Q", title="Count")],
        ).properties(height=220, background="transparent").configure_view(strokeOpacity=0)
        st.altair_chart(hist, use_container_width=True)
    else:
        st.caption("No Δ% data in current filter (rule-based detectors only log Δ% for price-change triggers).")


# ═══════════════════════════════════════════════════════════════════
# TAB 3 — ML SCORES
# ═══════════════════════════════════════════════════════════════════
with tab_ml:
    col_l, col_r = st.columns(2)

    with col_l:
        st.markdown("#### Isolation Forest anomaly score")
        st.markdown("""
<div class="info-box">
<strong>What is Isolation Forest?</strong><br>
A tree-based ML model that detects outliers without labeled training data.
It assigns each tick a score: higher = more isolated from normal behavior.
The model trains online from live data (first 300 ticks), then retrains every 500 ticks,
adapting to changing market regimes automatically.
</div>
""", unsafe_allow_html=True)
        st.markdown("")

        if_data = df_view.dropna(subset=["ts_dt"]).copy()
        if_data = if_data[if_data["if_score"] > 0]   # exclude rule-only events with no IF score
        if not if_data.empty:
            if_chart = alt.Chart(if_data).mark_circle(size=50, opacity=0.75).encode(
                x=alt.X("ts_dt:T", title=None,
                        axis=alt.Axis(format="%H:%M:%S", labelAngle=-30, labelColor="#6b7894",
                                      gridColor="#21262d")),
                y=alt.Y("if_score:Q", title="IF Anomaly Score",
                        axis=alt.Axis(labelColor="#6b7894", gridColor="#21262d")),
                color=alt.Color("severity:N",
                    scale=alt.Scale(domain=SEV_DOMAIN, range=SEV_RANGE), legend=None),
                tooltip=[alt.Tooltip("ts_dt:T",       format="%H:%M:%S"),
                         alt.Tooltip("if_score:Q",    title="IF Score",  format=".4f"),
                         alt.Tooltip("price:Q",        title="Price",    format="$,.2f"),
                         alt.Tooltip("severity:N"),
                         alt.Tooltip("primary_type:N", title="Detector")],
            ).properties(height=280, background="transparent").configure_view(strokeOpacity=0).interactive()
            st.altair_chart(if_chart, use_container_width=True)

            avg_if = if_data["if_score"].mean()
            max_if = if_data["if_score"].max()
            mc1, mc2 = st.columns(2)
            mc1.metric("Avg IF Score", f"{avg_if:.3f}", help="Average across all flagged events")
            mc2.metric("Max IF Score", f"{max_if:.3f}", help="Most anomalous event in this window")
        else:
            if not if_ready:
                warmup_left = max(300 - n_ticks, 0)
                st.markdown(f"""
<div class="info-box">
⏳ <strong>IF warming up — {warmup_left} more ticks needed.</strong><br><br>
The model needs 300 ticks of live data before its first training run.
At ~2 trades/second from Kraken, this takes about 2–3 minutes.
Rule-based detectors (pct_spike, abs_jump, vol_breakout) are active right now.
</div>
""", unsafe_allow_html=True)
                st.progress(min(n_ticks / 300, 1.0))
            else:
                st.info("IF is ready but no events in current filter have IF scores.")

    with col_r:
        st.markdown("#### LSTM reconstruction error")
        st.markdown("""
<div class="info-box">
<strong>What is the LSTM Autoencoder?</strong><br>
A deep learning model trained on 60-tick price sequences.
It learns what "normal" BTC price movement looks like,
then flags sequences where reconstruction error exceeds a dynamic threshold.
Score &gt; 1.0 means the pattern was more unusual than the threshold.
</div>
""", unsafe_allow_html=True)
        st.markdown("")

        lstm_data = df_view.dropna(subset=["ts_dt"]).copy()
        if lstm_data["lstm_score"].max() > 0:
            lstm_chart = alt.Chart(lstm_data).mark_circle(size=50, opacity=0.75).encode(
                x=alt.X("ts_dt:T", title=None,
                        axis=alt.Axis(format="%H:%M:%S", labelAngle=-30, labelColor="#6b7894",
                                      gridColor="#21262d")),
                y=alt.Y("lstm_score:Q", title="LSTM Error Ratio (threshold = 1.0)",
                        axis=alt.Axis(labelColor="#6b7894", gridColor="#21262d")),
                color=alt.Color("severity:N",
                    scale=alt.Scale(domain=SEV_DOMAIN, range=SEV_RANGE), legend=None),
                tooltip=[alt.Tooltip("ts_dt:T",      format="%H:%M:%S"),
                         alt.Tooltip("lstm_score:Q", title="Error Ratio", format=".4f"),
                         alt.Tooltip("price:Q",       title="Price",      format="$,.2f")],
            ).properties(height=280, background="transparent").configure_view(strokeOpacity=0).interactive()
            st.altair_chart(lstm_chart, use_container_width=True)
        else:
            st.markdown("""
<div class="info-box">
🔴 <strong>LSTM is not active.</strong><br><br>
The model needs to be trained on historical BTC data before it can run.
This is a one-time offline step:<br><br>
<code style='background:#0d1117;padding:2px 6px;border-radius:4px'>
cd consumer<br>
python collect_data.py --bars 5000<br>
python train_model.py<br>
docker compose up --build consumer
</code><br><br>
Once trained, the LSTM detects anomalies in 60-tick price sequences that
rule-based methods cannot see — gradual manipulation, regime changes, and
complex multi-tick patterns.
</div>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════
# TAB 4 — EVENT LOG
# ═══════════════════════════════════════════════════════════════════
with tab_table:
    col_h, col_dl = st.columns([4, 1])
    col_h.markdown(f"#### Anomaly event log — {len(df_view):,} events (newest first)")
    col_dl.markdown("")

    table = df_view[[
        "ts_dt","price","pct_change","abs_change",
        "severity","primary_type","if_score","lstm_score","types",
    ]].copy().iloc[::-1]   # newest first

    table["ts_dt"]      = table["ts_dt"].dt.strftime("%H:%M:%S")
    table["types"]      = table["types"].apply(lambda x: " · ".join(x))
    table["price"]      = table["price"].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "—")
    table["pct_change"] = table["pct_change"].apply(lambda x: f"{x:+.4f}%")
    table["abs_change"] = table["abs_change"].apply(lambda x: f"${x:+.2f}")
    table["if_score"]   = table["if_score"].apply(lambda x: f"{x:.3f}" if x > 0 else "—")
    table["lstm_score"] = table["lstm_score"].apply(lambda x: f"{x:.3f}" if x > 0 else "—")

    table = table.rename(columns={
        "ts_dt":        "Time (UTC)",
        "price":        "Price",
        "pct_change":   "Δ%",
        "abs_change":   "Δ$",
        "severity":     "Severity",
        "primary_type": "Main Detector",
        "if_score":     "IF Score",
        "lstm_score":   "LSTM Score",
        "types":        "All Detectors Fired",
    })

    st.dataframe(table, use_container_width=True, height=460)

    csv = table.to_csv(index=False).encode("utf-8")
    st.download_button(
        "💾 Download CSV",
        csv,
        "btc_anomalies.csv",
        "text/csv",
        help="Download all currently filtered events as a CSV file.",
    )
    st.caption(
        "**IF Score**: Isolation Forest anomaly score (higher = more unusual).  "
        "**LSTM Score**: reconstruction error ratio (>1.0 = anomaly).  "
        "**—** means the detector was not active or did not fire for that event."
    )
