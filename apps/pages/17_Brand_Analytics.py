import os, sys, datetime as dt
import streamlit as st
import pandas as pd
import plotly.express as px

_ROOT = os.path.dirname(os.path.dirname(__file__))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data_pipeline.bi.db import read_sql

st.set_page_config(page_title="Brand Analytics", layout="wide")
st.title("Brand Analytics Dashboard")

today = dt.date.today()
start_date = st.sidebar.date_input("Start date", value=today.replace(year=max(2015, today.year-1), month=1, day=1))
end_date = st.sidebar.date_input("End date", value=today)
start_s, end_s = start_date.isoformat(), end_date.isoformat()

@st.cache_data(ttl=300)
def brand_share(start: str, end: str):
    sql = """
      SELECT COALESCE(p.brand, t.brand, 'Unknown') AS brand, SUM(t.revenue) AS revenue
      FROM transactions t LEFT JOIN products p ON p.product_id = t.product_id
      JOIN time_dimension d ON d.date_key = t.date_key
      WHERE d.date BETWEEN %s AND %s
      GROUP BY 1 ORDER BY 2 DESC
    """
    return read_sql(sql, [start, end])

@st.cache_data(ttl=300)
def brand_trend(start: str, end: str):
    sql = """
      SELECT d.year, COALESCE(p.brand, t.brand, 'Unknown') AS brand, SUM(t.revenue) AS revenue
      FROM transactions t LEFT JOIN products p ON p.product_id = t.product_id
      JOIN time_dimension d ON d.date_key = t.date_key
      WHERE d.date BETWEEN %s AND %s
      GROUP BY d.year, brand ORDER BY d.year
    """
    return read_sql(sql, [start, end])

bs = brand_share(start_s, end_s)
if not bs.empty:
    st.plotly_chart(px.pie(bs.head(15), values='revenue', names='brand', title='Top Brand Share'), use_container_width=True)

bt = brand_trend(start_s, end_s)
if not bt.empty:
    st.plotly_chart(px.line(bt, x='year', y='revenue', color='brand', title='Brand Revenue Trend'), use_container_width=True)

