import os, sys, datetime as dt
import pandas as pd
import streamlit as st
import plotly.express as px

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Prime Membership Analytics", layout="wide")
st.title("Prime Membership Analytics")

today = dt.date.today()
start_date = st.sidebar.date_input("Start date", value=today.replace(year=max(2015, today.year-2), month=1, day=1))
end_date = st.sidebar.date_input("End date", value=today)
start_s, end_s = start_date.isoformat(), end_date.isoformat()

@st.cache_data(ttl=300)
def prime_metrics(start: str, end: str):
    sql = """
      SELECT COALESCE(t.is_prime_member, c.is_prime_member) AS prime,
             COUNT(*) AS orders,
             COUNT(DISTINCT t.customer_id) AS customers,
             SUM(t.revenue) AS revenue,
             AVG(t.revenue) AS aov
      FROM transactions t
      LEFT JOIN customers c ON c.customer_id = t.customer_id
      JOIN time_dimension d ON d.date_key = t.date_key
      WHERE d.date BETWEEN %s AND %s
      GROUP BY 1
    """
    return read_sql(sql, [start, end])

pm = prime_metrics(start_s, end_s)
if pm.empty:
    st.warning("No data available.")
    st.stop()

col1, col2, col3 = st.columns(3)
col1.metric("Prime Orders", f"{int(pm[pm['prime']==True]['orders'].sum()):,}")
col2.metric("Non-Prime Orders", f"{int(pm[pm['prime']==False]['orders'].sum()):,}")
col3.metric("Prime Revenue Share", f"{(pm[pm['prime']==True]['revenue'].sum() / max(1, pm['revenue'].sum()) * 100):.1f}%")

st.subheader("AOV by Prime Segment")
st.plotly_chart(px.bar(pm.fillna({'prime':'Unknown'}), x='prime', y='aov'), use_container_width=True)

@st.cache_data(ttl=300)
def prime_category_mix(start: str, end: str):
    sql = """
      SELECT COALESCE(t.is_prime_member, c.is_prime_member) AS prime,
             COALESCE(p.category, t.category, 'Unknown') AS category,
             SUM(t.revenue) AS revenue
      FROM transactions t
      LEFT JOIN products p ON p.product_id = t.product_id
      LEFT JOIN customers c ON c.customer_id = t.customer_id
      JOIN time_dimension d ON d.date_key = t.date_key
      WHERE d.date BETWEEN %s AND %s
      GROUP BY 1,2
    """
    return read_sql(sql, [start, end])

mix = prime_category_mix(start_s, end_s)
if not mix.empty:
    st.subheader("Category Mix by Prime Segment")
    fig = px.area(mix, x='category', y='revenue', color='prime', groupnorm='fraction')
    st.plotly_chart(fig, use_container_width=True)

