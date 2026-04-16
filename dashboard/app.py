import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

st.set_page_config(
    page_title="Medical Supply Chain Analytics",
    page_icon="+",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .block-container { padding-top: 1.5rem; }
    .stMetric { background: var(--background-color); }
    .story-box {
        background: #f0f7ff;
        border-left: 3px solid #378ADD;
        border-radius: 0 8px 8px 0;
        padding: 0.75rem 1rem;
        margin-bottom: 0.75rem;
        font-size: 13px;
        color: #1a1a2e;
    }
    .story-title {
        font-weight: 600;
        font-size: 13px;
        color: #185FA5;
        margin-bottom: 4px;
    }
</style>
""", unsafe_allow_html=True)

NEON_URL = "postgresql://neondb_owner:npg_NpQ4SulBe3nq@ep-frosty-field-amxfexd9-pooler.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require"

@st.cache_data
def load_orders():
    engine = create_engine(NEON_URL)
    return pd.read_sql("SELECT * FROM staging.mart_order_fulfillment", engine)

@st.cache_data
def load_delays():
    engine = create_engine(NEON_URL)
    return pd.read_sql("SELECT * FROM staging.mart_shipment_delays", engine)

@st.cache_data
def load_fraud():
    engine = create_engine(NEON_URL)
    return pd.read_sql("SELECT * FROM staging.mart_fraud_signals", engine)

# Load data
orders = load_orders()
delays = load_delays()
fraud = load_fraud()
orders['order_date'] = pd.to_datetime(orders['order_date'])

# ── SIDEBAR ──────────────────────────────────────────────
st.sidebar.title("Medical Supply Chain")
st.sidebar.markdown("---")

# Date filter
st.sidebar.markdown("**Date range**")
date_range = st.sidebar.date_input(
    "",
    value=[orders['order_date'].min(), orders['order_date'].max()],
    min_value=orders['order_date'].min(),
    max_value=orders['order_date'].max()
)

# Status filter — toggle buttons
st.sidebar.markdown("**Delivery status** · click to toggle")
all_statuses = ['on_time', 'late', 'not_delivered']
status_colors = {
    'on_time': '🟢',
    'late': '🟡',
    'not_delivered': '🔴'
}

selected_statuses = []
for s in all_statuses:
    key = f"btn_{s}"
    if key not in st.session_state:
        st.session_state[key] = True
    col_btn, col_label = st.sidebar.columns([1, 4])
    with col_btn:
        if st.button(status_colors[s], key=f"toggle_{s}"):
            st.session_state[key] = not st.session_state[key]
    with col_label:
        label = s.replace('_', ' ').title()
        if st.session_state[key]:
            st.markdown(f"**{label}** ✔")
        else:
            st.markdown(f"~~{label}~~")
    if st.session_state[key]:
        selected_statuses.append(s)

# If nothing selected, show all
if not selected_statuses:
    selected_statuses = all_statuses

st.sidebar.markdown("---")

# Story panel
st.sidebar.markdown("**Dashboard story**")
st.sidebar.markdown("""
<div class="story-box">
  <div class="story-title">What are we measuring?</div>
  This dashboard tracks 99K+ medical supply orders across delivery performance, logistics delays, and payment fraud.
</div>
<div class="story-box">
  <div class="story-title">The main finding</div>
  On-time delivery dropped below the 80% target after Jan 2018. Critical delays (7+ days) are the primary driver.
</div>
<div class="story-box">
  <div class="story-title">Fraud signal</div>
  Mastercard transactions flag at 23% — nearly 2× the platform average. Needs targeted controls.
</div>
<div class="story-box">
  <div class="story-title">Growth context</div>
  Order volume grew 18% YoY through 2017 but stalled sharply in Aug 2018. Root cause unknown.
</div>
""", unsafe_allow_html=True)

st.sidebar.markdown("---")
st.sidebar.markdown("**Pipeline**")
st.sidebar.success("S3 → PostgreSQL → dbt → Prefect")
st.sidebar.caption("Auto-updated daily")

# ── FILTER DATA ───────────────────────────────────────────
if len(date_range) == 2:
    mask = (
        (orders['order_date'].dt.date >= date_range[0]) &
        (orders['order_date'].dt.date <= date_range[1]) &
        (orders['delivery_status'].isin(selected_statuses))
    )
    filtered = orders[mask]
else:
    filtered = orders[orders['delivery_status'].isin(selected_statuses)]

# ── HEADER ────────────────────────────────────────────────
st.title("Medical Supply Chain · Analytics Dashboard")
st.markdown("Real-time insights across orders, logistics, and fraud signals")
st.markdown("---")

# ── EMPTY STATE GUARD ─────────────────────────────────────
if filtered.empty:
    st.warning("No orders found for the selected date range and filters. Try adjusting the date range or enabling more delivery statuses.")
    st.stop()

# ── KPI ROW ───────────────────────────────────────────────
total = len(filtered)
on_time = len(filtered[filtered['delivery_status'] == 'on_time'])
late = len(filtered[filtered['delivery_status'] == 'late'])
critical = len(delays[delays['delay_category'] == 'critical'])
fraud_rate = round(fraud['fraud_rate_pct'].mean(), 1)
on_time_rate = round(on_time / total * 100, 1) if total > 0 else 0
late_pct = round(late / total * 100, 1) if total > 0 else 0

col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.metric("Total orders", f"{total:,}")
with col2:
    st.metric("On-time rate", f"{on_time_rate}%",
              delta=f"{round(on_time_rate - 80, 1)}% vs 80% target",
              delta_color="normal")
with col3:
    st.metric("Late orders", f"{late:,}",
              delta=f"{late_pct}% of total",
              delta_color="inverse")
with col4:
    st.metric("Critical delays", f"{critical:,}",
              delta="7+ days late",
              delta_color="inverse")
with col5:
    st.metric("Avg fraud rate", f"{fraud_rate}%",
              delta="Mastercard highest",
              delta_color="inverse")

st.markdown("---")

# ROW 1: Volume + Donut
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Order volume over time")
    st.caption("Stacked by delivery status — green = on time, amber = late, red = not delivered")
    weekly = filtered.groupby([
        pd.Grouper(key='order_date', freq='W'),
        'delivery_status'
    ]).size().reset_index(name='count')
    fig1 = px.bar(
        weekly, x='order_date', y='count', color='delivery_status',
        color_discrete_map={
            'on_time': '#1D9E75',
            'late': '#EF9F27',
            'not_delivered': '#E24B4A'
        },
        barmode='stack',
        labels={'order_date': 'Week', 'count': 'Orders', 'delivery_status': 'Status'}
    )
    fig1.update_layout(
        height=350, legend=dict(orientation="h", y=-0.25),
        plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=10, b=10)
    )
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.subheader("Delivery breakdown")
    st.caption("Share of orders by status")
    status_counts = filtered['delivery_status'].value_counts()
    fig2 = px.pie(
        values=status_counts.values,
        names=status_counts.index,
        color=status_counts.index,
        color_discrete_map={
            'on_time': '#1D9E75',
            'late': '#EF9F27',
            'not_delivered': '#E24B4A'
        },
        hole=0.55
    )
    fig2.update_layout(
        height=350, legend=dict(orientation="h", y=-0.15),
        plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=10, b=10)
    )
    st.plotly_chart(fig2, use_container_width=True)

# ROW 2: Delays + Fraud
col1, col2 = st.columns(2)

with col1:
    st.subheader("Delay severity analysis")
    st.caption("Avg days late per category · labels show order count")
    delay_cat = delays.groupby('delay_category').agg(
        count=('order_id', 'count'),
        avg_days=('days_late', 'mean')
    ).reset_index()
    fig3 = px.bar(
        delay_cat, x='avg_days', y='delay_category', orientation='h',
        color='delay_category',
        color_discrete_map={
            'critical': '#E24B4A',
            'moderate': '#D85A30',
            'minor': '#EF9F27',
            'none': '#1D9E75'
        },
        text='count',
        labels={'avg_days': 'Avg days late', 'delay_category': 'Severity'}
    )
    fig3.update_traces(texttemplate='%{text} orders', textposition='outside')
    fig3.update_layout(
        height=300, showlegend=False,
        plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=10, b=10)
    )
    st.plotly_chart(fig3, use_container_width=True)

with col2:
    st.subheader("Fraud rate by card type")
    st.caption("% of transactions flagged · red = high risk")
    fraud_clean = fraud[fraud['card_type'].notna()].groupby('card_type').agg(
        fraud_rate=('fraud_rate_pct', 'mean'),
        total=('total_transactions', 'sum')
    ).reset_index().sort_values('fraud_rate', ascending=False)
    fig4 = px.bar(
        fraud_clean, x='card_type', y='fraud_rate',
        color='fraud_rate',
        color_continuous_scale=['#1D9E75', '#EF9F27', '#E24B4A'],
        text='fraud_rate',
        labels={'card_type': 'Card type', 'fraud_rate': 'Fraud rate (%)'}
    )
    fig4.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
    fig4.update_layout(
        height=300, showlegend=False, coloraxis_showscale=False,
        plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=10, b=10)
    )
    st.plotly_chart(fig4, use_container_width=True)

# ROW 3: Trend + Insights
st.markdown("---")
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Monthly on-time rate trend")
    st.caption("Dashed red line = 80% target · hover for exact values")
    monthly = filtered.copy()
    monthly['month'] = monthly['order_date'].dt.to_period('M').astype(str)
    monthly_rate = monthly.groupby('month').apply(
        lambda x: round(len(x[x['delivery_status'] == 'on_time']) / len(x) * 100, 1)
        if len(x) > 0 else 0
    ).reset_index(name='on_time_rate')
    fig5 = go.Figure()
    fig5.add_trace(go.Scatter(
        x=monthly_rate['month'], y=monthly_rate['on_time_rate'],
        mode='lines+markers',
        line=dict(color='#1D9E75', width=2.5),
        marker=dict(size=6),
        fill='tozeroy',
        fillcolor='rgba(29,158,117,0.1)',
        name='On-time rate'
    ))
    fig5.add_hline(
        y=80, line_dash="dash", line_color="#E24B4A",
        annotation_text="80% target", annotation_position="right"
    )
    fig5.update_layout(
        height=300,
        yaxis=dict(range=[0, 105], title='On-time rate (%)'),
        xaxis=dict(title='Month'),
        plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=10, b=10)
    )
    st.plotly_chart(fig5, use_container_width=True)

with col2:
    st.subheader("Key insights")
    st.error("3,247 critical delays need immediate vendor review")
    st.warning("Mastercard fraud rate 2x higher than Amex")
    st.success("18% YoY order growth — platform scaling well")
    st.info("On-time rate below 80% target since Jan 2018")

# Raw data
st.markdown("---")
with st.expander("View raw order data"):
    cols = ['order_id', 'order_date', 'delivery_status', 'days_late',
            'payment_amount', 'payment_type']
    available = [c for c in cols if c in filtered.columns]
    st.dataframe(filtered[available].head(500), use_container_width=True)

st.caption("Pipeline: CSV → AWS S3 (Parquet) → PostgreSQL → dbt → Streamlit · Automated via Prefect")