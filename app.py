import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="BioGenome Pipeline",
    page_icon="🧬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================
# CUSTOM STYLING
# ============================================================
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&family=JetBrains+Mono:wght@400;600&display=swap');

    html, body, [class*="css"] {
        font-family: 'Space Grotesk', sans-serif;
    }
    .main { background-color: #0a0e1a; }
    .stApp { background-color: #0a0e1a; }

    .metric-card {
        background: linear-gradient(135deg, #0f1829 0%, #1a2540 100%);
        border: 1px solid #1e3a5f;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        margin: 4px;
    }
    .metric-value {
        font-size: 2.2rem;
        font-weight: 700;
        color: #00d4ff;
        font-family: 'JetBrains Mono', monospace;
    }
    .metric-label {
        font-size: 0.8rem;
        color: #8899aa;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        margin-top: 4px;
    }
    .metric-sub {
        font-size: 0.75rem;
        color: #4a9eff;
        margin-top: 2px;
        font-family: 'JetBrains Mono', monospace;
    }

    .pipeline-stage {
        background: #0f1829;
        border: 1px solid #1e3a5f;
        border-radius: 8px;
        padding: 12px 16px;
        text-align: center;
        color: #8899aa;
        font-size: 0.8rem;
    }
    .pipeline-stage.active {
        border-color: #00d4ff;
        color: #00d4ff;
        box-shadow: 0 0 12px rgba(0,212,255,0.15);
    }
    .pipeline-arrow {
        color: #1e3a5f;
        font-size: 1.4rem;
        display: flex;
        align-items: center;
        justify-content: center;
    }

    .section-header {
        font-size: 0.75rem;
        font-weight: 600;
        color: #4a9eff;
        text-transform: uppercase;
        letter-spacing: 2px;
        margin: 24px 0 12px 0;
        padding-bottom: 8px;
        border-bottom: 1px solid #1e3a5f;
    }
    .badge-high { color: #ff4d6d; font-weight: 600; }
    .badge-med  { color: #ffd166; font-weight: 600; }
    .badge-low  { color: #06d6a0; font-weight: 600; }

    .stream-log {
        background: #040810;
        border: 1px solid #1e3a5f;
        border-radius: 8px;
        padding: 16px;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.75rem;
        color: #4a9eff;
        height: 180px;
        overflow-y: auto;
    }
    .log-line { margin: 2px 0; }
    .log-ok   { color: #06d6a0; }
    .log-warn { color: #ffd166; }
    .log-info { color: #4a9eff; }
</style>
""", unsafe_allow_html=True)

# ============================================================
# SNOWFLAKE CONNECTION (reads from st.secrets in Streamlit Cloud)
# ============================================================
@st.cache_resource
def get_snowflake_connection():
    return snowflake.connector.connect(
        account   = st.secrets["snowflake"]["account"],
        user      = st.secrets["snowflake"]["user"],
        password  = st.secrets["snowflake"]["password"],
        database  = "BIOGENOME_DB",
        schema    = "GENOMICS",
        warehouse = "BIOGENOME_WH"
    )

@st.cache_data(ttl=30)
def load_summary():
    conn = get_snowflake_connection()
    return pd.read_sql("SELECT * FROM VARIANT_SUMMARY_VIEW", conn)

@st.cache_data(ttl=30)
def load_recent(limit=50):
    conn = get_snowflake_connection()
    return pd.read_sql(f"""
        SELECT EVENT_ID, SAMPLE_ID, GENE_NAME, CHROMOSOME,
               VARIANT_TYPE, QUALITY_SCORE, READ_DEPTH,
               RISK_LEVEL, IS_PATHOGENIC, DISEASE_TAG, PROCESSED_AT
        FROM PROCESSED_VARIANTS
        ORDER BY PROCESSED_AT DESC
        LIMIT {limit}
    """, conn)

@st.cache_data(ttl=30)
def load_stream_lag():
    conn = get_snowflake_connection()
    df = pd.read_sql("SELECT COUNT(*) AS PENDING FROM GENE_EVENTS_STREAM", conn)
    return df["PENDING"][0]

@st.cache_data(ttl=30)
def load_totals():
    conn = get_snowflake_connection()
    df = pd.read_sql("""
        SELECT
            COUNT(*) AS TOTAL,
            SUM(CASE WHEN IS_PATHOGENIC THEN 1 ELSE 0 END) AS PATHOGENIC,
            COUNT(DISTINCT SAMPLE_ID) AS SAMPLES,
            ROUND(AVG(QUALITY_SCORE), 1) AS AVG_QUALITY
        FROM PROCESSED_VARIANTS
    """, conn)
    return df.iloc[0]

# ============================================================
# SIDEBAR
# ============================================================
with st.sidebar:
    st.markdown("### 🧬 BioGenome")
    st.markdown("**Real-time Genomic Variant Pipeline**")
    st.markdown("---")
    st.markdown("**Stack**")
    st.markdown("""
    - 🟠 **Kafka** — Event streaming
    - ❄️ **Snowflake Streams** — CDC
    - 🐍 **Python** — Orchestration
    - 📊 **Streamlit** — Dashboard
    """)
    st.markdown("---")
    auto_refresh = st.toggle("Auto-refresh (30s)", value=False)
    if st.button("🔄 Refresh Now"):
        st.cache_data.clear()
        st.rerun()
    st.markdown("---")
    st.markdown(f"<small style='color:#4a6080'>Last updated:<br>{datetime.now().strftime('%H:%M:%S')}</small>", unsafe_allow_html=True)

if auto_refresh:
    time.sleep(30)
    st.cache_data.clear()
    st.rerun()

# ============================================================
# HEADER
# ============================================================
st.markdown("""
<div style='padding: 32px 0 16px 0;'>
    <div style='font-size:0.75rem; color:#4a9eff; letter-spacing:3px; text-transform:uppercase; margin-bottom:8px;'>
        Cloud Modernization · AWS · Snowflake · Bioinformatics
    </div>
    <div style='font-size:2.4rem; font-weight:700; color:#e8f4ff; line-height:1.1;'>
        BioGenome Variant Pipeline
    </div>
    <div style='font-size:1rem; color:#8899aa; margin-top:8px;'>
        Kafka → Snowflake Streams → Real-time analytics for genomic variant discovery
    </div>
</div>
""", unsafe_allow_html=True)

# ============================================================
# PIPELINE FLOW DIAGRAM
# ============================================================
st.markdown('<div class="section-header">Pipeline Architecture</div>', unsafe_allow_html=True)
col1, col2, col3, col4, col5, col6, col7 = st.columns([3,1,3,1,3,1,3])
with col1:
    st.markdown('<div class="pipeline-stage active">🟠 Kafka Simulator<br><small>gene-variant-events</small></div>', unsafe_allow_html=True)
with col2:
    st.markdown('<div class="pipeline-arrow">→</div>', unsafe_allow_html=True)
with col3:
    st.markdown('<div class="pipeline-stage active">❄️ RAW_GENE_EVENTS<br><small>Snowflake Landing</small></div>', unsafe_allow_html=True)
with col4:
    st.markdown('<div class="pipeline-arrow">→</div>', unsafe_allow_html=True)
with col5:
    st.markdown('<div class="pipeline-stage active">📡 Snowflake Stream<br><small>CDC · Change Capture</small></div>', unsafe_allow_html=True)
with col6:
    st.markdown('<div class="pipeline-arrow">→</div>', unsafe_allow_html=True)
with col7:
    st.markdown('<div class="pipeline-stage active">✅ PROCESSED_VARIANTS<br><small>Curated · Enriched</small></div>', unsafe_allow_html=True)

# ============================================================
# KPI METRICS
# ============================================================
st.markdown('<div class="section-header">Live Metrics</div>', unsafe_allow_html=True)

try:
    totals = load_totals()
    stream_pending = load_stream_lag()

    m1, m2, m3, m4, m5 = st.columns(5)
    with m1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{int(totals['TOTAL']):,}</div>
            <div class="metric-label">Total Variants</div>
            <div class="metric-sub">Processed</div>
        </div>""", unsafe_allow_html=True)
    with m2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value" style="color:#ff4d6d">{int(totals['PATHOGENIC']):,}</div>
            <div class="metric-label">Pathogenic</div>
            <div class="metric-sub">Flagged variants</div>
        </div>""", unsafe_allow_html=True)
    with m3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value" style="color:#06d6a0">{int(totals['SAMPLES']):,}</div>
            <div class="metric-label">Unique Samples</div>
            <div class="metric-sub">Patient IDs</div>
        </div>""", unsafe_allow_html=True)
    with m4:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value" style="color:#ffd166">{totals['AVG_QUALITY']}</div>
            <div class="metric-label">Avg Quality</div>
            <div class="metric-sub">Phred-scaled score</div>
        </div>""", unsafe_allow_html=True)
    with m5:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value" style="color:#a78bfa">{int(stream_pending):,}</div>
            <div class="metric-label">Stream Pending</div>
            <div class="metric-sub">Unprocessed CDC events</div>
        </div>""", unsafe_allow_html=True)

except Exception as e:
    st.warning(f"⚠️ Could not load metrics: {e}")
    st.info("Make sure your Snowflake credentials are set in `.streamlit/secrets.toml`")

# ============================================================
# CHARTS
# ============================================================
st.markdown('<div class="section-header">Variant Analytics</div>', unsafe_allow_html=True)

try:
    df_summary = load_summary()

    c1, c2 = st.columns(2)

    with c1:
        fig1 = px.bar(
            df_summary.groupby("VARIANT_TYPE")["VARIANT_COUNT"].sum().reset_index(),
            x="VARIANT_TYPE", y="VARIANT_COUNT",
            title="Variants by Type",
            color="VARIANT_COUNT",
            color_continuous_scale=["#1e3a5f", "#00d4ff"],
            template="plotly_dark"
        )
        fig1.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#8899aa",
            title_font_color="#e8f4ff",
            showlegend=False,
            coloraxis_showscale=False
        )
        st.plotly_chart(fig1, use_container_width=True)

    with c2:
        df_disease = df_summary[df_summary["DISEASE_TAG"].notna()]
        fig2 = px.pie(
            df_disease.groupby("DISEASE_TAG")["PATHOGENIC_COUNT"].sum().reset_index(),
            values="PATHOGENIC_COUNT", names="DISEASE_TAG",
            title="Pathogenic Variants by Disease",
            color_discrete_sequence=px.colors.sequential.Blues_r,
            template="plotly_dark"
        )
        fig2.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#8899aa",
            title_font_color="#e8f4ff"
        )
        st.plotly_chart(fig2, use_container_width=True)

    c3, c4 = st.columns(2)

    with c3:
        fig3 = px.bar(
            df_summary.groupby("RISK_LEVEL")["VARIANT_COUNT"].sum().reset_index(),
            x="RISK_LEVEL", y="VARIANT_COUNT",
            title="Confidence Level Distribution",
            color="RISK_LEVEL",
            color_discrete_map={
                "HIGH_CONFIDENCE": "#00d4ff",
                "MEDIUM_CONFIDENCE": "#ffd166",
                "LOW_CONFIDENCE": "#ff4d6d"
            },
            template="plotly_dark"
        )
        fig3.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#8899aa",
            title_font_color="#e8f4ff",
            showlegend=False
        )
        st.plotly_chart(fig3, use_container_width=True)

    with c4:
        fig4 = px.bar(
            df_summary.groupby("CHROMOSOME")["VARIANT_COUNT"].sum().reset_index().sort_values("VARIANT_COUNT", ascending=False).head(10),
            x="CHROMOSOME", y="VARIANT_COUNT",
            title="Top 10 Chromosomes by Variant Count",
            color="VARIANT_COUNT",
            color_continuous_scale=["#1e3a5f", "#a78bfa"],
            template="plotly_dark"
        )
        fig4.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#8899aa",
            title_font_color="#e8f4ff",
            showlegend=False,
            coloraxis_showscale=False
        )
        st.plotly_chart(fig4, use_container_width=True)

    st.dataframe(df_recent, use_container_width=True, height=300)
except Exception as e:
    st.warning(f"Charts unavailable: {e}")

# ============================================================
# RECENT EVENTS TABLE
# ============================================================
st.markdown('<div class="section-header">Recent Variant Events (via Snowflake Stream)</div>', unsafe_allow_html=True)

try:
    df_recent = load_recent(50)

    def style_risk(val):
        if val == "HIGH_CONFIDENCE": return "color: #00d4ff; font-weight: 600"
        if val == "MEDIUM_CONFIDENCE": return "color: #ffd166; font-weight: 600"
        return "color: #ff4d6d; font-weight: 600"

    def style_pathogenic(val):
        return "color: #ff4d6d; font-weight: 600" if val else "color: #4a6080"
        

except Exception as e:
    st.warning(f"Table unavailable: {e}")

# ============================================================
# FOOTER
# ============================================================
st.markdown("""
<div style='margin-top:40px; padding-top:20px; border-top:1px solid #1e3a5f; text-align:center; color:#4a6080; font-size:0.75rem;'>
    BioGenome Pipeline · Built with Snowflake Streams + Python + Streamlit · Cloud Modernization Showcase
</div>
""", unsafe_allow_html=True)
