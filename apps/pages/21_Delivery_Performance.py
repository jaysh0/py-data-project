import os, sys, datetime as dt
import streamlit as st
import pandas as pd
import plotly.express as px

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Delivery Performance", layout="wide")
st.title("Delivery Performance Dashboard")

today = dt.date.today()
start_date = st.sidebar.date_input("Start date", value=today.replace(year=max(2015, today.year-1), month=1, day=1))
end_date = st.sidebar.date_input("End date", value=today)
on_time_threshold = st.sidebar.slider("On-time threshold (days)", 0, 15, 3)
start_s, end_s = start_date.isoformat(), end_date.isoformat()

@st.cache_data(ttl=300)
def delivery_stats(start: str, end: str):
    sql = """
      SELECT delivery_days FROM transactions t JOIN time_dimension d ON d.date_key=t.date_key
      WHERE d.date BETWEEN %s AND %s AND delivery_days IS NOT NULL
    """
    return read_sql(sql, [start, end])

ds = delivery_stats(start_s, end_s)
if not ds.empty:
    st.subheader("Delivery Days Distribution")
    st.plotly_chart(px.histogram(ds, x='delivery_days', nbins=30), use_container_width=True)

@st.cache_data(ttl=300)
def on_time_rate(start: str, end: str, thr: int):
    sql = """
      SELECT state, city,
             AVG(CASE WHEN delivery_days <= %s THEN 1.0 ELSE 0.0 END) AS on_time
      FROM transactions t JOIN time_dimension d ON d.date_key = t.date_key
      WHERE d.date BETWEEN %s AND %s AND delivery_days IS NOT NULL
      GROUP BY state, city ORDER BY on_time DESC
    """
    return read_sql(sql, [thr, start, end])

ot = on_time_rate(start_s, end_s, on_time_threshold)
if not ot.empty:
    st.subheader("On-time Rate by City")
    st.dataframe(ot.head(50))

